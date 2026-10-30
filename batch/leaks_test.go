package batch

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/fossas/faktory-plugins/expire"
	"github.com/fossas/faktory-plugins/uniq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getStatus(t *testing.T, cl *client.Client, bid string) client.BatchStatus {
	t.Helper()
	result, err := cl.Generic("BATCH STATUS " + bid)
	require.NoError(t, err)
	var status client.BatchStatus
	require.NoError(t, json.Unmarshal([]byte(result), &status))
	return status
}

func unfinishedCount(t *testing.T, s *server.Server, bid string) int64 {
	t.Helper()
	n, err := s.Manager().Redis().Get(context.Background(), batchUnfinishedKey(bid)).Int64()
	require.NoError(t, err)
	return n
}

func batchSubsystem(t *testing.T, s *server.Server) *BatchSubsystem {
	t.Helper()
	for _, sub := range s.Subsystems {
		if b, ok := sub.(*BatchSubsystem); ok {
			return b
		}
	}
	t.Fatal("batch subsystem not registered")
	return nil
}

func TestUniqRejectionRollsBackCounters(t *testing.T) {
	withServerConfig(true, func(s *server.Server, cl *client.Client) {
		result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)

		job1 := client.NewJob("UniqueJob", 1)
		job1.SetCustom("bid", bid)
		job1.SetCustom("unique_for", 60)
		require.NoError(t, cl.Push(job1))

		// Same queue, jobtype and args, so uniq rejects it after batch has seen it.
		job2 := client.NewJob("UniqueJob", 1)
		job2.SetCustom("bid", bid)
		job2.SetCustom("unique_for", 60)
		err = cl.Push(job2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NOTUNIQUE")

		status := getStatus(t, cl, bid)
		assert.Equal(t, int64(1), status.Total, "rejected push should not count toward total")
		assert.Equal(t, int64(1), status.Pending, "rejected push should not count toward pending")
		assert.Equal(t, int64(1), unfinishedCount(t, s, bid), "rejected push should not count as unfinished")

		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		fetched, err := cl.Fetch("default")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.NoError(t, cl.Ack(fetched.Jid))

		callback, err := cl.Fetch("callbacks")
		require.NoError(t, err)
		require.NotNil(t, callback, "success callback should fire once the accepted job is ACKed")
		assert.Equal(t, "SuccessCallback", callback.Type)
	}, new(uniq.UniqSubsystem))
}

func TestExpiredBatchJobReleasesCount(t *testing.T) {
	withServerConfig(true, func(s *server.Server, cl *client.Client) {
		result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"},"success":{"jobtype":"SuccessCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)

		job := client.NewJob("ExpiringJob", 1)
		job.SetCustom("bid", bid)
		job.SetCustom("expires_at", time.Now().Add(-time.Minute).Format(time.RFC3339Nano))
		require.NoError(t, cl.Push(job))

		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		// expire discards the job at fetch, so the worker gets nothing.
		fetched, err := cl.Fetch("default")
		require.NoError(t, err)
		require.Nil(t, fetched)

		status := getStatus(t, cl, bid)
		assert.Equal(t, int64(0), status.Pending, "expired job should no longer be pending")
		assert.Equal(t, int64(1), status.Failed, "expired job never ran, so it counts as failed")
		assert.Equal(t, int64(0), unfinishedCount(t, s, bid), "expired job is finished")

		callback, err := cl.Fetch("callbacks")
		require.NoError(t, err)
		require.NotNil(t, callback, "complete callback should fire after the only job expired")
		assert.Equal(t, "CompleteCallback", callback.Type)
	}, new(expire.ExpireSubsystem))
}

// fakeMiddlewareCtx lets a test drive a middleware with a job of its choosing.
type fakeMiddlewareCtx struct {
	job *client.Job
	mgr manager.Manager
}

func (f fakeMiddlewareCtx) Job() *client.Job                  { return f.job }
func (f fakeMiddlewareCtx) Reservation() *manager.Reservation { return nil }
func (f fakeMiddlewareCtx) Manager() manager.Manager          { return f.mgr }

func TestDiscardedRetryOnlyCountsFailure(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		ctx := context.Background()
		b := batchSubsystem(t, s)

		result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"callbacks"}}`)
		require.NoError(t, err)
		bid := string(result)

		job := client.NewJob("ExpiringJob", 1)
		job.SetCustom("bid", bid)
		require.NoError(t, cl.Push(job))

		// Simulate the job's first failure: pending was decremented then.
		rds := s.Manager().Redis()
		require.NoError(t, rds.Decr(ctx, batchPendingKey(bid)).Err())
		job.Failure = &client.Failure{RetryCount: 1, RetryRemaining: 5}

		mctx := context.WithValue(ctx, manager.MiddlewareHelperKey, fakeMiddlewareCtx{job: job, mgr: s.Manager()})
		err = b.fetchMiddleware(mctx, func() error { return manager.Discard("job expired") })
		require.Error(t, err)

		status := getStatus(t, cl, bid)
		assert.Equal(t, int64(0), status.Pending, "a discarded retry must not decrement pending again")
		assert.Equal(t, int64(1), status.Failed, "a discarded retry is a terminal failure")
		assert.Equal(t, int64(0), unfinishedCount(t, s, bid), "a discarded retry is finished")
	})
}

func TestCallbackCopiesReserveFor(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","queue":"callbacks","reserve_for":3600}}`)
		require.NoError(t, err)
		bid := string(result)

		_, err = cl.Generic("BATCH COMMIT " + bid)
		require.NoError(t, err)

		callback, err := cl.Fetch("callbacks")
		require.NoError(t, err)
		require.NotNil(t, callback)
		assert.Equal(t, 3600, callback.ReserveFor)
	})
}

func TestSweepReapsStaleCommittedBatches(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		ctx := context.Background()
		rds := s.Manager().Redis()
		b := batchSubsystem(t, s)
		old := util.Thens(time.Now().Add(-31 * 24 * time.Hour))

		// newStuckBatch commits a batch whose only job is never fetched,
		// standing in for a batch with a leaked pending count.
		newStuckBatch := func(def string) string {
			result, err := cl.Generic("BATCH NEW " + def)
			require.NoError(t, err)
			bid := string(result)
			job := client.NewJob("NeverRuns", bid)
			job.Queue = "never"
			job.SetCustom("bid", bid)
			require.NoError(t, cl.Push(job))
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)
			return bid
		}

		stale := newStuckBatch(`{"success":{"jobtype":"SuccessCallback"}}`)
		require.NoError(t, rds.HSet(ctx, batchMetaKey(stale), "committed_at", old).Err())

		// Batches committed before committed_at existed fall back to created_at.
		legacy := newStuckBatch(`{"success":{"jobtype":"SuccessCallback"}}`)
		require.NoError(t, rds.HDel(ctx, batchMetaKey(legacy), "committed_at").Err())
		require.NoError(t, rds.HSet(ctx, batchMetaKey(legacy), "created_at", old).Err())

		fresh := newStuckBatch(`{"success":{"jobtype":"SuccessCallback"}}`)

		// A stale child is left for its parent's reap, which deletes the tree.
		parent := newStuckBatch(`{"success":{"jobtype":"SuccessCallback"}}`)
		child := newStuckBatch(`{"success":{"jobtype":"SuccessCallback"},"parent_bid":"` + parent + `"}`)
		require.NoError(t, rds.HSet(ctx, batchMetaKey(child), "committed_at", old).Err())

		task := &batchSweepTask{subsystem: b}
		require.NoError(t, task.Execute(ctx))

		for _, bid := range []string{stale, legacy} {
			exists, err := batchExists(ctx, s, bid)
			require.NoError(t, err)
			assert.False(t, exists, "stale batch %s should be reaped", bid)
			member, err := rds.SIsMember(ctx, batchCommittedSetKey(), bid).Result()
			require.NoError(t, err)
			assert.False(t, member, "stale batch %s should leave the committed set", bid)
		}
		for _, bid := range []string{fresh, parent, child} {
			exists, err := batchExists(ctx, s, bid)
			require.NoError(t, err)
			assert.True(t, exists, "batch %s should not be reaped", bid)
		}

		require.NoError(t, rds.HSet(ctx, batchMetaKey(parent), "committed_at", old).Err())
		require.NoError(t, task.Execute(ctx))
		for _, bid := range []string{parent, child} {
			exists, err := batchExists(ctx, s, bid)
			require.NoError(t, err)
			assert.False(t, exists, "batch %s should be reaped with its tree", bid)
		}
	})
}

func TestAckAndFailDoNotRecreateDeletedBatch(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		ctx := context.Background()
		rds := s.Manager().Redis()

		batchKeys := func(bid string) []string {
			return []string{
				batchMetaKey(bid), batchTotalKey(bid), batchPendingKey(bid), batchFailedKey(bid),
				batchCompleteStateKey(bid), batchSuccessStateKey(bid), batchChildrenKey(bid),
			}
		}
		assertNoKeys := func(t *testing.T, bid string) {
			t.Helper()
			// Middleware may check callbacks in a goroutine; give it a moment.
			time.Sleep(200 * time.Millisecond)
			n, err := rds.Exists(ctx, batchKeys(bid)...).Result()
			require.NoError(t, err)
			assert.Equal(t, int64(0), n, "keys of deleted batch %s were recreated", bid)
		}

		t.Run("member ACK", func(t *testing.T) {
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)
			job := client.NewJob("TestJob", 1)
			job.Queue = "ack-deleted"
			job.SetCustom("bid", bid)
			require.NoError(t, cl.Push(job))

			fetched, err := cl.Fetch("ack-deleted")
			require.NoError(t, err)
			require.NotNil(t, fetched)
			require.NoError(t, deleteBatch(ctx, s, bid))
			require.NoError(t, cl.Ack(fetched.Jid))
			assertNoKeys(t, bid)
		})

		t.Run("member FAIL", func(t *testing.T) {
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)
			job := client.NewJob("TestJob", 1)
			job.Queue = "fail-deleted"
			job.Retry = new(int) // 0 retries: first FAIL is also terminal
			job.SetCustom("bid", bid)
			require.NoError(t, cl.Push(job))

			fetched, err := cl.Fetch("fail-deleted")
			require.NoError(t, err)
			require.NotNil(t, fetched)
			require.NoError(t, deleteBatch(ctx, s, bid))
			require.NoError(t, cl.Fail(fetched.Jid, assert.AnError, nil))
			assertNoKeys(t, bid)
		})

		t.Run("callback ACK", func(t *testing.T) {
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","queue":"cb-deleted"}}`)
			require.NoError(t, err)
			bid := string(result)
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			callback, err := cl.Fetch("cb-deleted")
			require.NoError(t, err)
			require.NotNil(t, callback)
			require.NoError(t, deleteBatch(ctx, s, bid))
			require.NoError(t, cl.Ack(callback.Jid))
			assertNoKeys(t, bid)
		})
	})
}
