mod future;
mod rusotologs;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use color_eyre::eyre::Result;
use linemux::MuxedLines;
use rusoto_logs::{CloudWatchLogsClient, InputLogEvent};
use stopper::Stopper;
use structopt::StructOpt;
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::{channel, Sender};
use tokio::time::{sleep, Duration};
use waitgroup::{WaitGroup, Worker};

#[derive(Debug, StructOpt)]
struct Opt {
    /// Echo to stdout
    #[structopt(long, short)]
    pub echo: bool,
    /// CloudWatch log group name
    #[structopt(long, short = "g")]
    pub log_group: String,
    /// CloudWatch log stream name
    #[structopt(long, short = "s")]
    pub log_stream: String,
    /// Files to follow. Empty or `-` means stdin.
    #[structopt(default_value = "-")]
    pub files: Vec<PathBuf>,
}

/// Returns current unix timestamp in milliseconds
fn timestamp() -> i64 {
    use std::convert::TryFrom;
    use std::time::SystemTime;
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap(),
        Err(err) => -i64::try_from(err.duration().as_millis()).unwrap(),
    }
}

async fn read_stdin(echo: bool, sender: Sender<InputLogEvent>, stopper: Stopper, wgw: Worker) {
    let mut stdin = BufReader::new(stdin()).lines();
    while let Some(Ok(Some(line))) = stopper.stop_future(stdin.next_line()).await {
        if line.is_empty() {
            continue;
        }
        if echo {
            println!("{}", line);
        }
        if sender
            .send(InputLogEvent {
                message: line,
                timestamp: timestamp(),
            })
            .await
            .is_err()
        {
            break;
        }
        sleep(Duration::from_millis(1)).await;
    }
    drop(wgw);
}

async fn read_files(
    mut file_watcher: MuxedLines,
    echo: bool,
    sender: Sender<InputLogEvent>,
    stopper: Stopper,
    wgw: Worker,
) {
    while let Some(Ok(Some(line))) = stopper.stop_future(file_watcher.next_line()).await {
        let line = line.line().to_string();
        if line.is_empty() {
            continue;
        }
        if echo {
            println!("{}", line);
        }
        if sender
            .send(InputLogEvent {
                message: line,
                timestamp: timestamp(),
            })
            .await
            .is_err()
        {
            break;
        }
        sleep(Duration::from_millis(1)).await;
    }
    drop(wgw);
}

async fn try_main(opt: Opt, stopper: Stopper, wg: &WaitGroup) -> Result<()> {
    let mut file_watcher = MuxedLines::new()?;
    let client = Arc::new(CloudWatchLogsClient::new(Default::default()));

    let (sender, receiver) = channel(crate::rusotologs::CLOUDWATCH_MAX_BATCH_EVENTS_LENGTH);

    ctrlc::set_handler({
        let stopper = stopper.clone();
        move || stopper.stop()
    })?;

    let mut files: HashSet<_> = opt.files.into_iter().collect();
    if files.remove(&PathBuf::from("-")) {
        tokio::spawn(read_stdin(
            opt.echo,
            sender.clone(),
            stopper.clone(),
            wg.worker(),
        ));
    }
    for file in files {
        if !file.exists() {
            tokio::fs::write(&file, "").await?;
        }
        file_watcher.add_file(&file).await?;
    }
    tokio::spawn(read_files(
        file_watcher,
        opt.echo,
        sender.clone(),
        stopper.clone(),
        wg.worker(),
    ));

    crate::rusotologs::rusoto_worker_loop(client, receiver, opt.log_group, opt.log_stream, stopper)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    let stopper = Stopper::new();
    let wg = WaitGroup::new();
    let res = try_main(opt, stopper.clone(), &wg).await;
    stopper.stop();
    wg.wait();
    res
}
