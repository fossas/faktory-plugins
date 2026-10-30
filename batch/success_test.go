package batch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRetryableBatch creates a committed batch with complete and success
// callbacks and a single member that has the given number of retries.
func newRetryableBatch(t *testing.T, cl *client.Client, retries int) string {
	t.Helper()
	result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"},"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
	require.NoError(t, err)
	bid := string(result)

	job := client.NewJob("RetryableJob", 1)
	job.SetCustom("bid", bid)
	job.Retry = &retries
	require.NoError(t, cl.Push(job))

	_, err = cl.Generic("BATCH COMMIT " + bid)
	require.NoError(t, err)
	return bid
}

// failAndRetry fetches the member from the default queue, FAILs it and moves
// it from the retry set back to its queue.
func failAndRetry(t *testing.T, s *server.Server, cl *client.Client) {
	t.Helper()
	fetched, err := cl.Fetch("default")
	require.NoError(t, err)
	require.NotNil(t, fetched)
	require.NoError(t, cl.Fail(fetched.Jid, fmt.Errorf("transient error"), nil))
	_, err = s.Manager().RetryJobs(context.Background(), time.Now().Add(time.Hour))
	require.NoError(t, err)
}

// waitForCallback waits for a callback job to be enqueued. Callbacks are pushed
// from a goroutine after the ACK or FAIL that triggers them returns.
func waitForCallback(t *testing.T, cl *client.Client) *client.Job {
	t.Helper()
	var callback *client.Job
	require.Eventually(t, func() bool {
		job, err := cl.Fetch("callbacks")
		callback = job
		return err == nil && job != nil
	}, 5*time.Second, 20*time.Millisecond, "expected a callback to be enqueued")
	return callback
}

// assertNoCallback checks the batch's callbacks synchronously, so any callback
// its counters allow has been enqueued, then asserts that none was.
func assertNoCallback(t *testing.T, s *server.Server, cl *client.Client, bid string, msg string) {
	t.Helper()
	batchSubsystem(t, s).checkAndFireCallbacks(context.Background(), bid)
	callback, err := cl.Fetch("callbacks")
	require.NoError(t, err)
	assert.Nil(t, callback, msg)
}

func TestSuccessWaitsForRetryingMember(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		bid := newRetryableBatch(t, cl, 5)

		failAndRetry(t, s, cl)

		callback := waitForCallback(t, cl)
		require.NotNil(t, callback, "complete fires once every member has run once")
		assert.Equal(t, "CompleteCallback", callback.Type)
		require.NoError(t, cl.Ack(callback.Jid))
		assertNoCallback(t, s, cl, bid, "success must wait while a member is still retrying")

		retried, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, retried)
		require.NoError(t, cl.Ack(retried.Jid))

		callback = waitForCallback(t, cl)
		require.NotNil(t, callback, "success fires once the retrying member is ACKed")
		assert.Equal(t, "SuccessCallback", callback.Type)
		assert.Equal(t, bid, callback.Custom["_bid"])
	})
}

func TestSuccessNeverFiresWhenRetryingMemberDies(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		bid := newRetryableBatch(t, cl, 1)

		failAndRetry(t, s, cl)

		callback := waitForCallback(t, cl)
		require.NotNil(t, callback)
		assert.Equal(t, "CompleteCallback", callback.Type)
		assertNoCallback(t, s, cl, bid, "success must wait while a member is still retrying")

		// The last retry fails, so the member dies.
		retried, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, retried)
		require.NoError(t, cl.Fail(retried.Jid, fmt.Errorf("permanent error"), nil))

		assertNoCallback(t, s, cl, bid, "success must not fire after a member died")
		status := getStatus(t, cl, bid)
		assert.Equal(t, int64(1), status.Failed)
	})
}

func TestUnfinishedCounter(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		ctx := context.Background()
		rds := s.Manager().Redis()
		unfinished := func(bid string) int64 {
			t.Helper()
			return unfinishedCount(t, s, bid)
		}

		result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)
		assert.Equal(t, int64(0), unfinished(bid))

		push := func(queue string, retries int) {
			t.Helper()
			job := client.NewJob("CountedJob", retries)
			job.Queue = queue
			job.SetCustom("bid", bid)
			job.Retry = &retries
			require.NoError(t, cl.Push(job))
		}
		push("default", 5) // retries, then succeeds
		push("default", 0) // dies on its first failure
		push("holding", 5) // keeps the batch open until the end
		assert.Equal(t, int64(3), unfinished(bid), "each push counts as unfinished")

		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)
		ttl, err := rds.TTL(ctx, batchUnfinishedKey(bid)).Result()
		require.NoError(t, err)
		assert.Equal(t, time.Duration(-1), ttl, "commit persists the counter")

		failAndRetry(t, s, cl)
		assert.Equal(t, int64(3), unfinished(bid), "a FAIL with retries left leaves the member unfinished")

		dies, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, dies)
		require.NoError(t, cl.Fail(dies.Jid, fmt.Errorf("no retries"), nil))
		assert.Equal(t, int64(2), unfinished(bid), "a terminal FAIL finishes the member")

		retried, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, retried)
		require.NoError(t, cl.Ack(retried.Jid))
		assert.Equal(t, int64(1), unfinished(bid), "an ACK on a retry finishes the member")

		holding, err := cl.Fetch("holding")
		require.NoError(t, err)
		require.NotNil(t, holding)
		require.NoError(t, cl.Ack(holding.Jid))

		// One member died, so success can never fire and the batch is cleaned up.
		assert.Eventually(t, func() bool {
			exists, err := rds.Exists(ctx, batchUnfinishedKey(bid)).Result()
			return err == nil && exists == 0
		}, 5*time.Second, 20*time.Millisecond, "cleanup deletes the counter")
	})
}

// A batch created before the unfinished counter existed has no counter key.
// It keeps the old rule, so its success callback does not wait for retries.
func TestBatchWithoutUnfinishedCounterKeepsOldRule(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		ctx := context.Background()
		rds := s.Manager().Redis()

		result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)
		require.NoError(t, rds.Del(ctx, batchUnfinishedKey(bid)).Err())

		retries := 5
		job := client.NewJob("RetryableJob", 1)
		job.SetCustom("bid", bid)
		job.Retry = &retries
		require.NoError(t, cl.Push(job))
		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		failAndRetry(t, s, cl)

		callback := waitForCallback(t, cl)
		require.NotNil(t, callback, "a batch without the counter fires success after first attempts")
		assert.Equal(t, "SuccessCallback", callback.Type)

		exists, err := rds.Exists(ctx, batchUnfinishedKey(bid)).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), exists, "the counter must not be created for an existing batch")
	})
}
