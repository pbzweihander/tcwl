# tcwl, To CloudWatch Logs

stdin or files -> `tcwl` -> CloudWatch Logs

## Usage

```
$ some_application \
  | tcwl \
    --log-group <log-group> \
    --log-stream <log-stream> \
    -
$ tcwl \
    --log-group <log-group> \
    --log-stream <log-stream> \
    logs/some_log_file
$ some_application \
  | tcwl \
    --log-group <log-group> \
    --log-stream <log-stream> \
    - \
    logs/some_log_file
```

## [License](LICENSE)

`tcwl` is lisenced under the terms of [MIT license](https://spdx.org/licenses/MIT.html)
