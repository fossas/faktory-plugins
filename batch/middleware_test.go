package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
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
		redis := s.Manager().Redis()
		redis.Set(ctx, batchSuccessStateKey(bid), CallbackEnqueued, 0)

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

			// Wait a moment for middleware to process
			time.Sleep(100 * time.Millisecond)

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

			// Wait a moment for middleware to process
			time.Sleep(100 * time.Millisecond)

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

			// Wait a moment for middleware to process
			time.Sleep(100 * time.Millisecond)

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
		})
	})
}

