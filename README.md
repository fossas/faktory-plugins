# Faktory

Faktory is a tool for managing background jobs. It was created by Mike Perham and is available at [contribsys/faktory](https://github.com/contribsys/faktory).

## Features

### Unique Jobs

Unique Jobs are supported and follow the [official implementation](https://github.com/contribsys/faktory/wiki/Ent-Unique-Jobs).

#### Configuration (required)

Uniq settings are under the `[uniq]` configuration:

```
[uniq]
enabled = true # enables this plugin
```

### Dogstatsd Metrics

| Name                                                 | Type      | Description                                                                                                                          |
| ---------------------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| faktory.ops.connections                              | Gauge     | Faktory client network connections                                                                                                   |
| faktory.jobs.working                                 | Gauge     | Current number of jobs being processed                                                                                               |
| faktory.jobs.scheduled                               | Gauge     | Current number of scheduled jobs                                                                                                     |
| faktory.jobs.retries                                 | Gauge     | Current number of jobs to be retried                                                                                                 |
| faktory.jobs.dead                                    | Gauge     | Current number of dead jobs                                                                                                          |
| faktory.jobs.enqueued{queue}                         | Gauge     | Number of jobs in {queue}                                                                                                            |
| faktory.jobs.latency{queue}                          | Gauge     | The time between now and when the oldest queued job was enqueued                                                                     |
| faktory.jobs.pushed{queue, jobtype}                  | Counter   | Total number of jobs pushed                                                                                                          |
| faktory.jobs.fetched{queue, jobtype}                 | Counter   | Total number of jobs fetched                                                                                                         |
| faktory.jobs.processed{queue, jobtype, status, dead} | Histogram | Timing for jobs that have been ACKed or FAILed. `status` is one of `success` or `fail`. `dead` is a boolean present for failed jobs. |

#### Configuration (required)

Any file ending in .toml will be read as a configuration file for faktory. Here's an example:

```
[metrics]
enabled = true # enables this plugin
tags = ["tag1:value1", "tag2:value2"] # tags passed to datadog on every metric
```

Tags can also be set the with env variable `DD_TAGS="tagName:value,tagName:value"`

The address of the statsd server should be set in the `DD_AGENT_HOST` environment variable.

Specify the host with `DD_AGENT_HOST`

### Cron

Enables enqueueing of jobs with a cron format

#### Configuration

```
[cron_plugin]
  enabled = true
[[ cron ]]
schedule = "*/5 * * * *"
  [cron.job]
    type = "JobType"
    queue = "queue"
    retry = 3
    args = [1, 2, 3]
    [cron.job.custom]
      value = true
[[ cron ]]
schedule = "3 * * * * * * *" # quartz format
  [cron.job]
    type = "OtherJob"
    queue = "queue"
    retry = 3
    [[cron.job.args]]
      key = "value1"
      enabled = true
    [[cron.jobs.args]]
      key = "value2"
      enabled = false
    [cron.job.custom]
      value = true
```

Each `[[ cron ]]` entry must have:

`schedule`: a cron express, predefined schedule or intervals

`type`: Faktory Job type to be queued

#### Cron expressions

| Field name   | Mandatory? | Allowed values  | Allowed special characters |
| ------------ | ---------- | --------------- | -------------------------- |
| Seconds      | Yes        | 0-59            | \* / , -                   |
| Minutes      | Yes        | 0-59            | \* / , -                   |
| Hours        | Yes        | 0-23            | \* / , -                   |
| Day of month | Yes        | 1-31            | \* / , - ?                 |
| Month        | Yes        | 1-12 or JAN-DEC | \* / , -                   |
| Day of week  | Yes        | 0-6 or SUN-SAT  | \* / , - ?                 |

Predefined schedules

You may use one of several pre-defined schedules in place of a cron expression.

| Entry                  | Description                                | Equivalent To   |
| ---------------------- | ------------------------------------------ | --------------- |
| @yearly (or @annually) | Run once a year, midnight, Jan. 1st        | 0 0 0 1 1 \*    |
| @monthly               | Run once a month, midnight, first of month | 0 0 0 1 \* \*   |
| @weekly                | Run once a week, midnight between Sat/Sun  | 0 0 0 \* \* 0   |
| @daily (or @midnight)  | Run once a day, midnight                   | 0 0 0 \* \* \*  |
| @hourly                | Run once an hour, beginning of hour        | 0 0 \* \* \* \* |

Intervals

You may also schedule a job to execute at fixed intervals, starting at the time it's added or cron is run. This is supported by formatting the cron spec like this:

`@every <duration>`

where "duration" is a string accepted by time.ParseDuration (http://golang.org/pkg/time/#ParseDuration).

Passing job arguments

arguments for jobs can be passed using `args = [1,2,3]` or using `[[cron.job.args]]` and adding key, pair values.

### Expiring Jobs

Jobs can be configured to automatically expire after a preset time has passed by setting the `expires_at` custom attribute to an ISO8601 timestamp. Alternatively, you can set the `expires_in` custom attribute to a Golang Duration string to expire a job a given duration after it was enqueued.

### Requeue Jobs

Implements a `REQUEUE {"jid": "..."}` command that ACKs the given job and then immediately requeues it to the beginning of the queue. This can be useful if you need a worker to give up execution of a job without impacting the retry count, for example if the worker is scaling down.

## License

All code in this repository is licensed under the AGPL.
