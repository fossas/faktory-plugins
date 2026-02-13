package batch

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompleteCallback(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("fires when all jobs complete", func(t *testing.T) {
			// Create batch with complete callback
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
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

			// Fetch and ACK the job
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			err = cl.Ack(fetchedJob.Jid)
			require.NoError(t, err)

			// Check that callback was enqueued
			callbackJob, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callbackJob, "complete callback should be enqueued")

			assert.Equal(t, "CompleteCallback", callbackJob.Type)

			// Verify callback JID is a valid UUID
			_, err = uuid.Parse(callbackJob.Jid)
			assert.NoError(t, err, "callback job JID should be a valid UUID")

			// Verify callback metadata
			bidValue, ok := callbackJob.GetCustom("_bid")
			assert.True(t, ok)
			assert.Equal(t, bid, bidValue)

			cbType, ok := callbackJob.GetCustom("_cb")
			assert.True(t, ok)
			assert.Equal(t, "complete", cbType)
		})
	})
}

func TestSuccessCallback(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("fires when all jobs succeed", func(t *testing.T) {
			// Create batch with success callback
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
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

			// Fetch and ACK the job
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			err = cl.Ack(fetchedJob.Jid)
			require.NoError(t, err)

			// Check that callback was enqueued
			callbackJob, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callbackJob, "success callback should be enqueued")

			assert.Equal(t, "SuccessCallback", callbackJob.Type)
		})

		t.Run("does not fire when jobs fail", func(t *testing.T) {
			// Create batch with success callback
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push a job with no retries
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", bid)
			retry := 0
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

			err = cl.Fail(fetchedJob.Jid, fmt.Errorf("test failure"), nil)
			require.NoError(t, err)

			// Check that no callback was enqueued
			callbackJob, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			assert.Nil(t, callbackJob, "success callback should not fire when jobs fail")

			// Note: batch is cleaned up after terminal failure because:
			// - No complete callback defined (so complete is "finished")
			// - failed > 0 means success can never fire (so success is "finished")
			// We can verify the batch was cleaned up:
			_, err = cl.Generic("BATCH STATUS " + bid)
			assert.Error(t, err, "batch should be cleaned up after terminal failure with no complete callback")
			assert.Contains(t, err.Error(), "not found")
		})
	})
}

func TestCallbackOrdering(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("success enqueued after complete enqueued", func(t *testing.T) {
			// Both callbacks get enqueued in the same checkAndFireCallbacks call
			// because success fires when CompleteState == CallbackEnqueued
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks"},"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
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

			// Fetch and ACK the job
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			err = cl.Ack(fetchedJob.Jid)
			require.NoError(t, err)

			// Fetch first callback — should be complete
			first, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, first)
			assert.Equal(t, "CompleteCallback", first.Type)

			// Fetch second callback — success should already be enqueued
			second, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, second, "success should be enqueued right after complete")
			assert.Equal(t, "SuccessCallback", second.Type)
		})

		t.Run("success fires even if complete already finished", func(t *testing.T) {
			// Tests the path where CompleteState == CallbackFinished
			// (complete callback ACK'd before success is checked)
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks"},"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
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

			// Fetch and ACK the job
			fetchedJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob)

			err = cl.Ack(fetchedJob.Jid)
			require.NoError(t, err)

			// Fetch the complete callback and ACK it immediately
			completeCallback, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, completeCallback)
			assert.Equal(t, "CompleteCallback", completeCallback.Type)

			err = cl.Ack(completeCallback.Jid)
			require.NoError(t, err)

			// Success should fire because CompleteState == CallbackFinished
			successCallback, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, successCallback, "success should fire after complete finishes")
			assert.Equal(t, "SuccessCallback", successCallback.Type)
		})
	})
}

func TestEmptyBatchCallback(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("callbacks fire immediately for empty batch", func(t *testing.T) {
			// Create batch with callback
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Commit batch without pushing any jobs
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Check that callback was enqueued
			callbackJob, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callbackJob, "callback should fire immediately for empty batch")

			assert.Equal(t, "SuccessCallback", callbackJob.Type)
		})
	})
}

func TestCompleteCallbackWaitsForAllJobs(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("does not fire while job is still running even if another failed", func(t *testing.T) {
			// This tests the scenario:
			// - Batch has 2 jobs (A and B)
			// - Job A fails (first execution) -> pending goes 2 -> 1
			// - Job B is still running -> pending is still 1
			// - Complete callback should NOT fire yet
			// - Job B finishes (ACK) -> pending goes 1 -> 0
			// - Complete callback fires

			// Create batch with complete callback
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push two jobs with retries enabled
			jobA := client.NewJob("JobA", 1)
			jobA.SetCustom("bid", bid)
			retry := 5
			jobA.Retry = &retry
			err = cl.Push(jobA)
			require.NoError(t, err)

			jobB := client.NewJob("JobB", 2)
			jobB.SetCustom("bid", bid)
			jobB.Retry = &retry
			err = cl.Push(jobB)
			require.NoError(t, err)

			// Commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Verify initial state: total=2, pending=2
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)
			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)
			assert.Equal(t, int64(2), status.Total)
			assert.Equal(t, int64(2), status.Pending)

			// Fetch both jobs
			fetchedJobA, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJobA)

			fetchedJobB, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJobB)

			// Fail job A (first execution - pending should decrement to 1)
			err = cl.Fail(fetchedJobA.Jid, fmt.Errorf("job A failed"), nil)
			require.NoError(t, err)

			// Verify state: pending=1 (job A failed once, job B still running)
			statusResult, err = cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)
			assert.Equal(t, int64(2), status.Total)
			assert.Equal(t, int64(1), status.Pending, "pending should be 1 after first job fails")
			assert.Equal(t, int64(0), status.Failed, "failed should be 0 (job will retry)")

			// Complete callback should NOT be enqueued yet (pending > 0)
			callbackJob, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			assert.Nil(t, callbackJob, "complete callback should not fire while job B is still pending")

			// Now ACK job B (pending should go to 0)
			err = cl.Ack(fetchedJobB.Jid)
			require.NoError(t, err)

			// Verify state: pending=0
			statusResult, err = cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)
			assert.Equal(t, int64(0), status.Pending, "pending should be 0 after job B finishes")

			// Now complete callback should be enqueued
			callbackJob, err = cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callbackJob, "complete callback should fire after all jobs executed at least once")
			assert.Equal(t, "CompleteCallback", callbackJob.Type)
		})
	})
}

func TestNoDoubleCallback(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("callback fires exactly once", func(t *testing.T) {
			// Create batch with callback
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
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

			// Fetch and ACK both jobs
			fetchedJob1, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob1)

			fetchedJob2, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedJob2)

			// ACK both jobs quickly to potentially trigger race
			err = cl.Ack(fetchedJob1.Jid)
			require.NoError(t, err)
			err = cl.Ack(fetchedJob2.Jid)
			require.NoError(t, err)

			// Wait for callback to be enqueued
			time.Sleep(300 * time.Millisecond)

			// Should only have one callback
			callbackJob1, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callbackJob1, "callback should be enqueued")

			callbackJob2, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			assert.Nil(t, callbackJob2, "should only have one callback")
		})
	})
}
