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

Metrics tracked through a task (every 10 seconds) are:

`jobs.enqueued.{queueName}.count` - number of jobs in a queue by name
`jobs.enqueued.{queueName}.time` - current queue time for next job
`jobs.working.count` - number of jobs currently running
`jobs.scheduled.count` - number of jobs scheduled
`jobs.retries.count` - number of jobs in retry state
`jobs.dead.count` - number of dead jobs
`jobs.enqueued.count` - total number of enqueued jobs

#### Configuration

Any file ending in .toml will be read as a configuration file for faktory. Here's an example:
```
[metrics]
enabled = true # enables this plugin
namespace = "jobs" # changes the prefix for the metric from `jobs.` to the value specified
tags = ["tag1:value1", "tag2:value2"] # tags passed to datadog on every metric
```

Tags can also be set the with env variable `DD_TAGS="tagName:value,tagName:value"

specify the host with `DD_AGENT_HOST`

### Cron

Enables enqueueing of jobs with a cron format

### Configuration

```
[cron_plugin]
  enabled = true
[[ cron ]]
schedule = "*/5 * * * *"
  [cron.job]
    type = "JobType"
    queue = "queue"
    retry = 3
    [cron.job.custom]
      value = true
[[ cron ]]
schedule = "3 * * * * * * *" # quartz format
  [cron.job]
    type = "OtherJob"
    queue = "queue"
    retry = 3
    [cron.job.custom]
      value = true
```

schedule can be in quarts format (http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html), which includes seconds

## License

All code in this repository is licensed under the AGPL.
