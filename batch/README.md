Batches allows the developer to declare a set of jobs to perform as a group and fire a callback when all of those jobs are complete or successful. This is known as a batch of jobs. Batches can be nested into complex, dynamic workflows that implement your business processes.

## Basics

Faktory jobs execute concurrently, without any ordering guarantees or coordination. If you have a complex workflow to automate, Batches make it possible to enforce ordering. Consider a workflow with steps A -> B -> C. Each serial step in your workflow is a Batch of jobs. Once all the jobs in Batch A execute, a callback fires. The callback for Batch A will create Batch B and its jobs. All the jobs within a Batch run concurrently but Faktory guarantees that your callback will only run **after** all jobs are complete or successful.

## Definitions

* **complete** means that all jobs in the batch have executed once, no matter if they were ACK'd or FAIL'd.
* **success** means that all jobs executed and were ACK'd, i.e. no failures. If jobs repeatedly fail and don't retry or die, it's possible success will never fire.

## Batch Commands

All batch commands have the form `BATCH [subcommand]`.

## BATCH NEW {json}

Use the BATCH NEW command to create a batch with a JSON blob that represents either/both a success or complete callback job. The command will return the BID for the new batch.

```
bid = BATCH NEW {"description":"An optional description for the Web UI",
             "success":{"jobtype":"MySuccessCallback","args":[user_id],"queue":"critical"},
             "complete":...}
  jid = PUSH {...,"custom":{"bid":"b-xxxxxxxxx"}}
  jid = PUSH {...,"custom":{"bid":"b-xxxxxxxxx"}}
BATCH COMMIT <bid>
```

You then push as many jobs as necessary for the batch. Each job you push **MUST** have the BID placed into its "custom" data hash: `"custom":{"bid":"b-xxxxxxxxx"}`. Pushed jobs will start executing immediately but Faktory guarantees that the callbacks for a batch will not fire until the batch has been committed.

Although this simple example doesn't show it, you may nest batches arbitrarily deep allowing for complex nested workflows. The initial batch data has a TTL of 30 minutes and will expire if your code does not call BATCH COMMIT.

## BATCH COMMIT bid

Use `BATCH COMMIT <bid>` to commit the Batch, meaning that its definition is complete and all initial jobs within it have been pushed. Those jobs can reopen the batch and push more jobs into it if necessary.

If you don't push any jobs into the batch, any callbacks will fire immediately upon `BATCH COMMIT`.

## BATCH OPEN bid

You can dynamically add more jobs or nested, child batches to a batch by reopening it:

```
BATCH OPEN bid
  jid = PUSH {...,"custom":{"bid":bid}}
  childbid = BATCH NEW {"parent_bid":bid,...}
    childjid = PUSH {...,"custom":{"bid":childbid}}
  BATCH COMMIT childbid
BATCH COMMIT bid
```

Note that, once committed, only a job within the batch may reopen it.  Faktory will return an error if you dynamically add jobs from "outside" the batch; this is to prevent a race condition between callbacks firing and an outsider adding more jobs.

## Callbacks

The initial batch creation must include a success and/or a complete job callback.  A callback is job that Faktory will enqueue once the batch of jobs has completed or succeeded. You can specify `args`, `queue`, etc and a callback can fail and retry like any other job.

```
{"jobtype":"MySuccessCallback","args":[user_id],"queue":"critical"}
```

Batch jobs can run any time after the batch is created and the jobs block is finished but callbacks are guaranteed not to fire until the batch has been committed.

Batch jobs automatically have a custom `bid` attribute set with their associated Batch ID.

```json
{"jobtype":"BatchJob","args":[123],"custom":{"bid":"abc123"}}
```

Callback jobs automatically have custom `_bid` and `_cb` attributes set with their associated Batch ID and callback type.

```json
{"jobtype":"MySuccessCallback","args":[123],"custom":{"_bid":"abc123","_cb":"success"}}
```

## Status

You can retrieve the status of a batch using the BATCH STATUS command. This returns a JSON blob of data associated with the batch:

```
BATCH STATUS bid
{"bid":<bid>,"total":17,"pending":14,"failed":3,"created_at":"2019-11-18T13:48:25Z","description":"..."}
```

## Client API support

The Golang and Ruby packages both provide idiomatic APIs for creating and executing batches. Here's an example of the Ruby API creating a simple parent/child batch. The parent's `success` callback will not fire until the child's `success` callback has run successfully (i.e. the two BatchJobs will run concurrently but Faktory guarantees that BarJob will always run before FooJob):

```ruby
b = Faktory::Batch.new
b.success = FooJob.to_s
b.jobs do |parent|
  BatchJob.perform_async

  child = Faktory::Batch.new
  child.parent_bid = parent.bid
  child.success = BarJob.to_s
  child.jobs do
    BatchJob.perform_async
  end
end
```

### More Resources

* faktory_worker_ruby: [complex_batch.rb](https://github.com/contribsys/faktory_worker_ruby/blob/main/examples/complex_batch.rb), [batch_test.rb](https://github.com/contribsys/faktory_worker_ruby/blob/main/test/batch_test.rb)
* faktory_worker_go: [test/main.go](https://github.com/contribsys/faktory_worker_go/blob/main/test/main.go)

## Web UI

The Faktory Enterprise Web UI includes a Batches tab where you can get a list of currently defined batches and their status.

## Guarantees

* Callbacks for a batch will not enqueue until you call `BATCH COMMIT`.
* You may safely reopen and push more jobs for a batch **from jobs within that batch**.
* You may safely reopen and add child batches for a batch **from jobs within that batch**.
* Once a callback has enqueued for a batch, you may not add anything to the batch.
* The callback for a parent batch will not enqueue until the callback for the child batch has finished.
* The success callback for a batch will always enqueue after the complete callback.

## Side Note/Rant

There are many "enterprise" tools which provide visual "workflow" builder tools which market themselves as an easy way to build and manage business processes with the drag of a box. This is a fool's errand. These tools are inevitably sold to a CIO/CTO for millions of dollars. The affected teams try them, realize they don't handle the complexities/realities of business well and discard them to a shelf somewhere. I know this because I helped build one of these "magical" $$$$$ tools at IBM long ago. Today the OSS world is building those same types of tools and making the same mistakes.

Faktory Enterprise's Batches are a pragmatic response to those tools:

1. They are code
  * They deploy like any other code changes.
  * They can be reviewed as code.
  * Changes are visible in VCS.
  * They can be deployed and tested in QA/staging environments like any other code change.
2. Dynamic
  * Dynamically include jobs based on app logic
  * Dynamically add nested workflows based on app logic
3. Language-independent
  * Batches are just a set of jobs, and Faktory jobs can be implemented in any language
4. Scale
  * Add 100,000s of jobs to a batch, it will work<sup>*</sup>.

\* Just make sure Faktory has enough memory!
