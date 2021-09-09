use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use async_recursion::async_recursion;
use color_eyre::eyre::Result;
use rusoto_core::RusotoError;
use rusoto_logs::{
    CloudWatchLogs, CreateLogStreamRequest, InputLogEvent, PutLogEventsError, PutLogEventsRequest,
};

use stopper::Stopper;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;

use crate::future::poll_once;

pub const CLOUDWATCH_MAX_BATCH_EVENTS_LENGTH: usize = 10_000;
pub const CLOUDWATCH_MAX_BATCH_SIZE: usize = 1024 * 1024;
pub const CLOUDWATCH_EXTRA_MSG_PAYLOAD_SIZE: usize = 26;

pub async fn rusoto_worker_loop(
    client: Arc<impl CloudWatchLogs + Send + Sync>,
    channel: Receiver<InputLogEvent>,
    log_group: String,
    log_stream: String,
    stopper: Stopper,
) -> Result<()> {
    let mut sequence_token: Option<String> = None;
    let mut event_chunk = EventChunk::new(channel);
    while let Some(Some(log_events)) = stopper.stop_future(event_chunk.recv()).await {
        let request = PutLogEventsRequest {
            log_events,
            log_group_name: log_group.clone(),
            log_stream_name: log_stream.clone(),
            sequence_token: sequence_token.clone(),
        };
        sequence_token = put_log_events(client.as_ref(), request).await?;
    }
    Ok(())
}

#[async_recursion]
async fn put_log_events(
    client: &(impl CloudWatchLogs + Sync),
    request: PutLogEventsRequest,
) -> Result<Option<String>> {
    let resp = client.put_log_events(request.clone()).await;
    let err = match resp {
        Ok(resp) => return Ok(resp.next_sequence_token),
        Err(err) => err,
    };
    match &err {
        RusotoError::Service(PutLogEventsError::InvalidSequenceToken(msg)) => {
            if let Some(sequence_token) = try_get_sequence_token_from_error(msg) {
                return put_log_events(
                    client,
                    PutLogEventsRequest {
                        sequence_token,
                        ..request
                    },
                )
                .await;
            }
        }
        RusotoError::Service(PutLogEventsError::DataAlreadyAccepted(msg)) => {
            if let Some(sequence_token) = try_get_sequence_token_from_error(msg) {
                return put_log_events(
                    client,
                    PutLogEventsRequest {
                        sequence_token,
                        ..request
                    },
                )
                .await;
            }
        }
        RusotoError::Service(PutLogEventsError::ResourceNotFound(_)) => {
            client
                .create_log_stream(CreateLogStreamRequest {
                    log_group_name: request.log_group_name.clone(),
                    log_stream_name: request.log_stream_name.clone(),
                })
                .await?;
            return put_log_events(
                client,
                PutLogEventsRequest {
                    sequence_token: None,
                    ..request
                },
            )
            .await;
        }
        RusotoError::Unknown(resp) => {
            if let Ok(body) = std::str::from_utf8(&resp.body) {
                if body.contains("ThrottlingException") {
                    sleep(Duration::from_secs(1)).await;
                    return put_log_events(client, request).await;
                }
            }
        }
        _ => {}
    }
    Err(err.into())
}

struct EventChunk {
    channel: Receiver<InputLogEvent>,
    buffer: BTreeMap<i64, String>,
    buffer_size: usize,
}

impl EventChunk {
    fn new(channel: Receiver<InputLogEvent>) -> Self {
        Self {
            channel,
            buffer: BTreeMap::new(),
            buffer_size: 0,
        }
    }

    fn replace_buffer(&mut self, new_buffer: BTreeMap<i64, String>) -> Vec<InputLogEvent> {
        let buffer = mem::replace(&mut self.buffer, new_buffer);
        buffer
            .into_iter()
            .map(|(timestamp, message)| InputLogEvent { message, timestamp })
            .collect()
    }

    async fn recv(&mut self) -> Option<Vec<InputLogEvent>> {
        loop {
            let mut poll = Some(self.channel.recv().await);

            loop {
                match poll {
                    Some(Some(InputLogEvent { message, timestamp })) => {
                        let payload_size = message.len() + CLOUDWATCH_EXTRA_MSG_PAYLOAD_SIZE;

                        if self.buffer_size + payload_size >= CLOUDWATCH_MAX_BATCH_SIZE {
                            if !self.buffer.is_empty() {
                                let mut new_buffer = BTreeMap::new();
                                new_buffer.insert(timestamp, message);
                                self.buffer_size = payload_size;
                                return Some(self.replace_buffer(new_buffer));
                            } else {
                                self.buffer.insert(timestamp, message);
                                self.buffer_size = 0;
                                return Some(self.replace_buffer(BTreeMap::new()));
                            }
                        } else {
                            self.buffer.insert(timestamp, message);
                            self.buffer_size += payload_size;

                            if self.buffer.len() >= CLOUDWATCH_MAX_BATCH_EVENTS_LENGTH {
                                self.buffer_size = 0;
                                return Some(self.replace_buffer(BTreeMap::new()));
                            }

                            // Else continue
                        }
                    }
                    Some(None) => {
                        if !self.buffer.is_empty() {
                            self.buffer_size = 0;
                            return Some(self.replace_buffer(BTreeMap::new()));
                        }

                        // Pending
                        break;
                    }
                    None => {
                        self.buffer_size = 0;
                        return Some(self.replace_buffer(BTreeMap::new()));
                    }
                }

                poll = poll_once(self.channel.recv()).await;
            }
        }
    }
}

fn try_get_sequence_token_from_error(msg: &str) -> Option<Option<String>> {
    if let Some((_, sequence_token)) = msg.rsplit_once(" ") {
        let sequence_token = sequence_token.trim();
        if sequence_token == "null" {
            Some(None)
        } else {
            Some(Some(sequence_token.to_string()))
        }
    } else {
        None
    }
}
