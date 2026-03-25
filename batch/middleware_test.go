package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/fossas/faktory-plugins/requeue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPushMiddleware(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("increments counters for batch job", func(t *testing.T) {
			// Create batch
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push job with batch ID
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", bid)
			err = cl.Push(job)
			require.NoError(t, err)

			// Check counters
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)

			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)

			assert.Equal(t, int64(1), status.Total)
			assert.Equal(t, int64(1), status.Pending)
		})

		t.Run("passes through non-batch jobs", func(t *testing.T) {
			// Push job without batch ID
			job := client.NewJob("TestJob", 1)
			err := cl.Push(job)
			assert.NoError(t, err)
		})

		t.Run("rejects push to non-existent batch", func(t *testing.T) {
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", "non-existent-batch")
			err := cl.Push(job)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "does not exist")
		})
	})
}

func TestPushMiddlewareAfterCallbackStarted(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		ctx := context.Background()

		// Create and commit empty batch
		result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
		require.NoError(t, err)
		bid := string(result)

		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		// Manually set callback state to simulate callback started
		rds := s.Manager().Redis()
		rds.Set(ctx, batchSuccessStateKey(bid), CallbackEnqueued, 0)

		// Try to push job to batch after callback started
		job := client.NewJob("TestJob", 1)
		job.SetCustom("bid", bid)
		err = cl.Push(job)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "callbacks have started")
	})
}

func TestAckMiddleware(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("decrements pending on ACK", func(t *testing.T) {
			// Create batch
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push job with batch ID
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", bid)
			err = cl.Push(job)
			require.NoError(t, err)

			// Fetch and ACK the job
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			err = cl.Ack(fetchedJob.Jid)
			require.NoError(t, err)

			// Check counters
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)

			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)

			assert.Equal(t, int64(1), status.Total)
			assert.Equal(t, int64(0), status.Pending)
			assert.Equal(t, int64(0), status.Failed)
		})

		t.Run("passes through non-batch jobs", func(t *testing.T) {
			// Push non-batch job
			job := client.NewJob("NonBatchJob", 1)
			err := cl.Push(job)
			require.NoError(t, err)

			// Fetch and ACK
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			err = cl.Ack(fetchedJob.Jid)
			assert.NoError(t, err)
		})
	})
}

func TestDirectToMorgueFailure(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("Retry=-1 is detected as terminal failure", func(t *testing.T) {
			// BUG 1: Retry=-1 (direct-to-morgue) should be treated as terminal failure.
			// Use complete callback to keep batch alive for status check.
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"},"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push job with Retry=-1 (direct to morgue)
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", bid)
			retry := -1
			job.Retry = &retry
			err = cl.Push(job)
			require.NoError(t, err)

			// Commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Fetch and FAIL the job
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			err = cl.Fail(fetchedJob.Jid, fmt.Errorf("direct to morgue"), nil)
			require.NoError(t, err)

			// Check counters: failed should be 1 (terminal failure)
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)

			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)

			assert.Equal(t, int64(0), status.Pending, "pending should be 0 after first execution")
			assert.Equal(t, int64(1), status.Failed, "failed should be 1 for direct-to-morgue job")

			// Complete callback should fire (pending=0)
			completeCallback, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, completeCallback, "complete callback should fire")
			assert.Equal(t, "CompleteCallback", completeCallback.Type)
			bidValue, ok := completeCallback.GetCustom("_bid")
			assert.True(t, ok)
			assert.Equal(t, bid, bidValue)

			// Success callback should NOT fire since there's a failure
			successCallback, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			assert.Nil(t, successCallback, "success callback should not fire when job went to morgue")
		})
	})
}

func TestCallbackJobTerminalFailure(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("marks callback as finished when callback job terminally fails", func(t *testing.T) {
			// BUG 6: Callback job failure should be handled
			ctx := context.Background()

			// Create batch with both callbacks, complete callback has no retries
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks","retry":0},"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push a job
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", bid)
			err = cl.Push(job)
			require.NoError(t, err)

			// Commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Fetch and ACK the job to trigger complete callback
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)
			err = cl.Ack(fetchedJob.Jid)
			require.NoError(t, err)

			// Fetch the complete callback
			completeCallback, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, completeCallback, "complete callback should be enqueued")
			assert.Equal(t, "CompleteCallback", completeCallback.Type)

			// FAIL the complete callback (it has retry=0, so terminal)
			err = cl.Fail(completeCallback.Jid, fmt.Errorf("callback failed"), nil)
			require.NoError(t, err)

			// The complete callback state should be CallbackFinished despite failure
			rds := s.Manager().Redis()
			state, err := rds.Get(ctx, batchCompleteStateKey(bid)).Result()
			require.NoError(t, err)
			assert.Equal(t, CallbackFinished, state, "complete callback should be marked finished after terminal failure")

			// Success callback should still fire (pending=0, failed=0, complete finished)
			successCallback, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, successCallback, "success callback should fire after complete callback terminally fails")
			assert.Equal(t, "SuccessCallback", successCallback.Type)
		})
	})
}

func TestRetryThenSuccessDoesNotDoubleDecrementPending(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		ctx := context.Background()

		// Create batch with both callbacks
		result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"},"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)

		// Push job1 (retryable, on "default" queue) and job2 (on "holding" queue to keep batch alive)
		job1 := client.NewJob("RetryableJob", 1)
		job1.SetCustom("bid", bid)
		retry := 5
		job1.Retry = &retry
		job1.Queue = "default"
		err = cl.Push(job1)
		require.NoError(t, err)

		job2 := client.NewJob("HoldingJob", 1)
		job2.SetCustom("bid", bid)
		job2.Queue = "holding"
		err = cl.Push(job2)
		require.NoError(t, err)

		// Commit batch → pending=2
		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		// Fetch job1 from "default" and FAIL it → pending should go 2→1
		fetchedJob1, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, fetchedJob1)
		assert.Equal(t, job1.Jid, fetchedJob1.Jid)

		err = cl.Fail(fetchedJob1.Jid, fmt.Errorf("transient error"), nil)
		require.NoError(t, err)

		// Move job1 from retry set back to its queue
		_, err = s.Manager().RetryJobs(ctx, time.Now().Add(time.Hour))
		require.NoError(t, err)

		// Fetch retried job1 from "default" and ACK it
		retriedJob1, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, retriedJob1, "retried job1 should be back on the default queue")
		assert.Equal(t, job1.Jid, retriedJob1.Jid)

		err = cl.Ack(retriedJob1.Jid)
		require.NoError(t, err)

		// Check batch status
		statusResult, err := cl.Generic("BATCH STATUS " + bid)
		require.NoError(t, err)

		var status client.BatchStatus
		err = json.Unmarshal([]byte(statusResult), &status)
		require.NoError(t, err)

		assert.Equal(t, int64(1), status.Pending, "pending should be 1 (only job2 remains); double decrement bug if 0")
		assert.Equal(t, int64(0), status.Failed, "failed should be 0 (job1 eventually succeeded)")
	})
}

func TestFailMiddleware(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("increments failed on terminal failure", func(t *testing.T) {
			// Create batch
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push job with batch ID and no retries
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", bid)
			retry := 0
			job.Retry = &retry
			err = cl.Push(job)
			require.NoError(t, err)

			// Fetch and FAIL the job
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			err = cl.Fail(fetchedJob.Jid, fmt.Errorf("test failure"), nil)
			require.NoError(t, err)

			// Check counters
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)

			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)

			assert.Equal(t, int64(1), status.Total)
			assert.Equal(t, int64(0), status.Pending)
			assert.Equal(t, int64(1), status.Failed)
		})

		t.Run("decrements pending on first failure even with retries", func(t *testing.T) {
			// Create batch - complete callback fires when all jobs have executed at least once
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push job with batch ID and explicit retries
			job := client.NewJob("RetryableTestJob", 1)
			job.SetCustom("bid", bid)
			retry := 5
			job.Retry = &retry
			err = cl.Push(job)
			require.NoError(t, err)

			// Fetch and FAIL the job (first failure, with retries remaining)
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			err = cl.Fail(fetchedJob.Jid, fmt.Errorf("test failure"), nil)
			require.NoError(t, err)

			// Check counters:
			// - pending should be 0 because job has executed at least once
			// - failed should be 0 because job will be retried (not terminal)
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)

			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)

			assert.Equal(t, int64(1), status.Total)
			assert.Equal(t, int64(0), status.Pending, "pending should be 0 after first execution (even if failed)")
			assert.Equal(t, int64(0), status.Failed, "failed should be 0 when job will be retried")

			cl.Flush()
		})

		t.Run("does not increment failed on non-first non-terminal failure", func(t *testing.T) {
			ctx := context.Background()

			// Create batch with complete callback + a holding job to keep batch alive
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push retryable job
			job := client.NewJob("RetryableTestJob2", 1)
			job.SetCustom("bid", bid)
			retry := 5
			job.Retry = &retry
			job.Queue = "default"
			err = cl.Push(job)
			require.NoError(t, err)

			// Push holding job to keep batch alive
			holdingJob := client.NewJob("HoldingJob", 1)
			holdingJob.SetCustom("bid", bid)
			holdingJob.Queue = "holding"
			err = cl.Push(holdingJob)
			require.NoError(t, err)

			// Commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Fetch and FAIL the job (first failure) → pending decrements, failed stays 0
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)
			assert.Equal(t, job.Jid, fetchedJob.Jid)

			err = cl.Fail(fetchedJob.Jid, fmt.Errorf("transient error"), nil)
			require.NoError(t, err)

			// Move job from retry set back to its queue
			_, err = s.Manager().RetryJobs(ctx, time.Now().Add(time.Hour))
			require.NoError(t, err)

			// Fetch retried job and FAIL it again (second failure, still has retries)
			retriedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, retriedJob, "retried job should be back on the default queue")
			assert.Equal(t, job.Jid, retriedJob.Jid)

			err = cl.Fail(retriedJob.Jid, fmt.Errorf("transient error again"), nil)
			require.NoError(t, err)

			// Check counters: failed should still be 0 (non-first, non-terminal path)
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)

			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)

			assert.Equal(t, int64(0), status.Failed, "failed should be 0 on non-first non-terminal failure")
		})
	})
}

func TestRequeueDoesNotAffectBatchCounters(t *testing.T) {
	withServerConfig(true, func(s *server.Server, cl *client.Client) {
		// Create batch with a complete callback
		result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)

		// Push two jobs
		job1 := client.NewJob("TestJob", 1)
		job1.SetCustom("bid", bid)
		err = cl.Push(job1)
		require.NoError(t, err)

		job2 := client.NewJob("TestJob", 2)
		job2.SetCustom("bid", bid)
		err = cl.Push(job2)
		require.NoError(t, err)

		// Commit batch
		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		// Fetch job1 and REQUEUE it
		fetched, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		assert.Equal(t, job1.Jid, fetched.Jid)

		_, err = cl.Generic(fmt.Sprintf(`REQUEUE {"jid":%q}`, fetched.Jid))
		require.NoError(t, err)

		// Check counters: REQUEUE should not change total or pending
		statusResult, err := cl.Generic("BATCH STATUS " + bid)
		require.NoError(t, err)

		var status client.BatchStatus
		err = json.Unmarshal([]byte(statusResult), &status)
		require.NoError(t, err)

		assert.Equal(t, int64(2), status.Total, "total should remain 2 after REQUEUE")
		assert.Equal(t, int64(2), status.Pending, "pending should remain 2 after REQUEUE")
		assert.Equal(t, int64(0), status.Failed, "failed should remain 0 after REQUEUE")
	}, new(requeue.RequeueSubsystem))
}

func TestRequeueLastPendingJobDoesNotFireCallbacks(t *testing.T) {
	withServerConfig(true, func(s *server.Server, cl *client.Client) {
		// Create batch with a single job
		result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)

		job := client.NewJob("TestJob", 1)
		job.SetCustom("bid", bid)
		err = cl.Push(job)
		require.NoError(t, err)

		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		// Fetch and REQUEUE the only job
		fetched, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, fetched)

		_, err = cl.Generic(fmt.Sprintf(`REQUEUE {"jid":%q}`, fetched.Jid))
		require.NoError(t, err)

		// Callback should NOT have fired
		callback, err := cl.Fetch("callbacks")
		require.NoError(t, err)
		assert.Nil(t, callback, "complete callback should not fire after REQUEUE")

		// pending should still be 1
		statusResult, err := cl.Generic("BATCH STATUS " + bid)
		require.NoError(t, err)

		var status client.BatchStatus
		err = json.Unmarshal([]byte(statusResult), &status)
		require.NoError(t, err)

		assert.Equal(t, int64(1), status.Pending, "pending should still be 1")
	}, new(requeue.RequeueSubsystem))
}

func TestRequeueThenAckCompletesBatch(t *testing.T) {
	withServerConfig(true, func(s *server.Server, cl *client.Client) {
		// Create batch with a single job
		result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)

		job := client.NewJob("TestJob", 1)
		job.SetCustom("bid", bid)
		err = cl.Push(job)
		require.NoError(t, err)

		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		// Fetch and REQUEUE
		fetched, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, fetched)

		_, err = cl.Generic(fmt.Sprintf(`REQUEUE {"jid":%q}`, fetched.Jid))
		require.NoError(t, err)

		// Fetch the requeued job and ACK it
		fetched2, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, fetched2)
		assert.Equal(t, job.Jid, fetched2.Jid)

		err = cl.Ack(fetched2.Jid)
		require.NoError(t, err)

		// Allow time for the async callback check
		time.Sleep(100 * time.Millisecond)

		// Callback SHOULD fire now
		callback, err := cl.Fetch("callbacks")
		require.NoError(t, err)
		require.NotNil(t, callback, "complete callback should fire after REQUEUE then ACK")
		assert.Equal(t, "CompleteCallback", callback.Type)

		// Counters should be correct
		statusResult, err := cl.Generic("BATCH STATUS " + bid)
		require.NoError(t, err)

		var status client.BatchStatus
		err = json.Unmarshal([]byte(statusResult), &status)
		require.NoError(t, err)

		assert.Equal(t, int64(1), status.Total, "total should be 1")
		assert.Equal(t, int64(0), status.Pending, "pending should be 0 after ACK")
		assert.Equal(t, int64(0), status.Failed, "failed should be 0")
	}, new(requeue.RequeueSubsystem))
}

func TestRequeueMultipleTimesCountersCorrect(t *testing.T) {
	withServerConfig(true, func(s *server.Server, cl *client.Client) {
		// Create batch
		result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)

		job := client.NewJob("TestJob", 1)
		job.SetCustom("bid", bid)
		err = cl.Push(job)
		require.NoError(t, err)

		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		// REQUEUE 3 times
		for i := 0; i < 3; i++ {
			fetched, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetched)

			_, err = cl.Generic(fmt.Sprintf(`REQUEUE {"jid":%q}`, fetched.Jid))
			require.NoError(t, err)
		}

		// Counters should be unchanged after 3 requeues
		statusResult, err := cl.Generic("BATCH STATUS " + bid)
		require.NoError(t, err)

		var status client.BatchStatus
		err = json.Unmarshal([]byte(statusResult), &status)
		require.NoError(t, err)

		assert.Equal(t, int64(1), status.Total, "total should still be 1 after 3 requeues")
		assert.Equal(t, int64(1), status.Pending, "pending should still be 1 after 3 requeues")

		// Now ACK it
		fetched, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		err = cl.Ack(fetched.Jid)
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		statusResult, err = cl.Generic("BATCH STATUS " + bid)
		require.NoError(t, err)
		err = json.Unmarshal([]byte(statusResult), &status)
		require.NoError(t, err)

		assert.Equal(t, int64(1), status.Total, "total should be 1 after final ACK")
		assert.Equal(t, int64(0), status.Pending, "pending should be 0 after final ACK")
	}, new(requeue.RequeueSubsystem))
}
