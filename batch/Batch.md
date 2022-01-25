## Batching

Batching allows jobs to be batched together and queue a job when the entire batch has been completed or ran successfully. Batching also allows nested batch with complex relationships. A child batch can be added to multiple parents.


### Definitions

- `Committed` - the initial jobs and batch configuration have been submitted. This means once all the jobs/batches have finished processing the batch can fire its callbacks. Batches can be opened to add more jobs and batches if the jobs have not been processed.
- `Complete` - all the jobs/batches within the batch have been processed. This will fire whether all jobs succeed or not.
- `Success` - all the jobs/batches within the batch have been successfully ran. If a job is retried and succeeds this job will then fire. If a job never succeeds then no callback with fire.

### Configuration

Batching can be enabled with the follow config:

```
[batch]
enabled = true # enables the batch plugin
```

The following values can be also be used:

`child_search_depth` (optional) - the maximum depth used to track children, a value of 0 means infinite depht.
`uncommitted_timeout_minutes` (optional) - the number of minutes until an uncommitted batch times out. After this adding jobs/children to the batch will cause an error.
`committed_timeout_days` (optional) - the number of days until a committed batch times out. After this adding children to the batch will cause an error.


## Batch Commands

### `BATCH NEW {json}`
- returns a batch id. 

Creates a new uncommitted batch, the following attributes are supported:

- `description' (optional) - a description for the batch
- `child_search_depth` (optional) - overrides the default value for tracking the maximum nested child depth when determining if a batch is done
- `complete` - the job to fire when the batch is in complete state.
- `success` - the job to fire when the batch is in success state.


#### Callbacks

Callbacks will have a `_bid` (references batch id) and a `_cb` (either `success` or `complete`) within the `custom` values. The job definition follows the faktory spec. So jobs can be added to queues, have retries, and passed args.

### `BATCH COMMIT bid`

Commits a batch so that no new jobs / batches can be added.

### `BATCH OPEN bid`
- returns batch id, if successful

Attempts to open a committed batch, so that new jobs and sub batches can be added. If the batches jobs have been processed then this will return an error.

### `BATCH CHILD bid child_bid`

Attempts to add a child batch to a batch. The parent batch needs to be in an uncommitted state otherwise child batches cannot be added.

### `BATCH STATUS bid`
- returns a json blob with information about a batch.

The attributes returned are:

| attribute          | definition                                |
|--------------------|-------------------------------------------|
| bid`               | the batch Id                              |
| description        | the description of the batch              |
| total              | the total number of jobs within the batch |
| pending            | the number of pending jobs                |
| created_at         | time the the batch was created at         |
| completed_st       | complete job state*                       |
| success_st         | success job state*                        |
| child_search_depth | the depth to track child batches          |

* callback state
- "": No status
- "1": Job is pending
- "2": Job has succeeded