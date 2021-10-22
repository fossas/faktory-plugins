# Faktory

Faktory is a tool for managing background jobs. It was created by Mike Perham and is available at [contribsys/faktory](https://github.com/contribsys/faktory).

## Features

### Unique Jobs

Unique Jobs are supported and follow the [official implementation](https://github.com/contribsys/faktory/wiki/Ent-Unique-Jobs).

### Dogstatsd Metrics

Metrics are collected through a task and middleware.

Metrics tracked through middleware are:
`jobs.succeeded.count` - number of jobs successful
`jobs.succeeded.time` - time to complete
`jobs.failed.count` - number of jobs failed
`jobs.failed.time` - time to complete
`jobs.retried_at_least_once.count` - jobs with a retry > 0 that have failed once

Metrics tracked through a task (every 10 seconds) are:
`jobs.{queueName}.queued.count` - number of jobs in a queue by name
`jobs.{queueName}.queued.time` - current queue time for next job

to add tags set the env variable `DD_TAGS="tagName:value,tagName:value"

#### Configuration

Any file ending in .toml will be read as a configuration file for faktory. To enable a connection to the statsd server create a file and this:
```
[metrics]
statsd_server = "host:port"
```

## License

All code in this repository is licensed under the AGPL.
