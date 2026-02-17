package batch

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchNew(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("creates batch with success callback", func(t *testing.T) {
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback","args":[1]}}`)
			require.NoError(t, err)

			bid := string(result)
			assert.True(t, len(bid) > 0)
			assert.Contains(t, bid, "b-")
		})

		t.Run("creates batch with complete callback", func(t *testing.T) {
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback","args":[1]}}`)
			require.NoError(t, err)

			bid := string(result)
			assert.True(t, len(bid) > 0)
			assert.Contains(t, bid, "b-")
		})

		t.Run("creates batch with both callbacks", func(t *testing.T) {
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"},"complete":{"jobtype":"CompleteCallback"},"description":"test batch"}`)
			require.NoError(t, err)

			bid := string(result)
			assert.True(t, len(bid) > 0)
		})

		t.Run("rejects batch without callbacks", func(t *testing.T) {
			_, err := cl.Generic(`BATCH NEW {"description":"no callbacks"}`)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "at least one callback")
		})

		t.Run("rejects batch with client-specified BID", func(t *testing.T) {
			_, err := cl.Generic(`BATCH NEW {"bid":"my-bid","success":{"jobtype":"SuccessCallback"}}`)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "BID must be blank")
		})

		t.Run("rejects invalid JSON", func(t *testing.T) {
			_, err := cl.Generic(`BATCH NEW {invalid json}`)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid JSON")
		})

		t.Run("rejects missing data", func(t *testing.T) {
			_, err := cl.Generic(`BATCH NEW`)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing batch definition")
		})
	})
}

func TestBatchCommit(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("commits batch", func(t *testing.T) {
			// Create batch
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			assert.NoError(t, err)

			// Verify committed via status
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)

			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)

			// Check the batch is created with initial state
			assert.Equal(t, bid, status.Bid)
			assert.Equal(t, int64(0), status.Total)
			assert.Equal(t, int64(0), status.Pending)
		})

		t.Run("rejects non-existent batch", func(t *testing.T) {
			_, err := cl.Generic("BATCH COMMIT non-existent-bid")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})

		t.Run("rejects missing BID", func(t *testing.T) {
			_, err := cl.Generic("BATCH COMMIT")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing batch ID")
		})
	})
}

func TestBatchStatus(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("returns batch status", func(t *testing.T) {
			// Create and commit batch
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"},"description":"test batch"}`)
			require.NoError(t, err)
			bid := string(result)

			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Get status
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)

			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)

			assert.Equal(t, bid, status.Bid)
			assert.Equal(t, "test batch", status.Description)
			assert.Equal(t, int64(0), status.Total)
			assert.Equal(t, int64(0), status.Pending)
			assert.Equal(t, int64(0), status.Failed)
			assert.NotEmpty(t, status.CreatedAt)
		})

		t.Run("rejects non-existent batch", func(t *testing.T) {
			_, err := cl.Generic("BATCH STATUS non-existent-bid")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})

		t.Run("rejects missing BID", func(t *testing.T) {
			_, err := cl.Generic("BATCH STATUS")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing batch ID")
		})
	})
}

func TestBatchOpen(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("opens committed batch", func(t *testing.T) {
			// Create batch
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push a job so the batch doesn't immediately fire callbacks
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", bid)
			err = cl.Push(job)
			require.NoError(t, err)

			// Commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Open batch (job still pending so callbacks haven't started)
			openResult, err := cl.Generic("BATCH OPEN " + bid)
			assert.NoError(t, err)
			assert.Equal(t, bid, string(openResult))
		})

		t.Run("opens uncommitted batch", func(t *testing.T) {
			// Create batch but do NOT commit it
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// BATCH OPEN on a never-committed batch should succeed
			// (setUncommitted only checks callback states, not committed flag)
			openResult, err := cl.Generic("BATCH OPEN " + bid)
			assert.NoError(t, err)
			assert.Equal(t, bid, string(openResult))
		})

		t.Run("verifies state changes after open", func(t *testing.T) {
			ctx := context.Background()
			rds := s.Manager().Redis()

			// Create batch
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push a job so the batch doesn't immediately fire callbacks
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", bid)
			err = cl.Push(job)
			require.NoError(t, err)

			// Commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Verify batch is in committed set before open
			isMember, err := rds.SIsMember(ctx, batchCommittedSetKey(), bid).Result()
			require.NoError(t, err)
			assert.True(t, isMember, "batch should be in committed set before open")

			// Open batch
			_, err = cl.Generic("BATCH OPEN " + bid)
			require.NoError(t, err)

			// Verify committed field is "0"
			committed, err := rds.HGet(ctx, batchMetaKey(bid), "committed").Result()
			require.NoError(t, err)
			assert.Equal(t, "0", committed, "committed flag should be cleared to '0'")

			// Verify batch removed from committed set
			isMember, err = rds.SIsMember(ctx, batchCommittedSetKey(), bid).Result()
			require.NoError(t, err)
			assert.False(t, isMember, "batch should be removed from committed set after open")

			// Verify TTL is restored on batch keys
			// Note: children key only exists if the batch has child batches,
			// so we check the 6 keys that are always present.
			keys := []string{
				batchMetaKey(bid),
				batchTotalKey(bid),
				batchPendingKey(bid),
				batchFailedKey(bid),
				batchCompleteStateKey(bid),
				batchSuccessStateKey(bid),
			}
			for _, key := range keys {
				ttl, err := rds.TTL(ctx, key).Result()
				require.NoError(t, err, "failed to get TTL for %s", key)
				assert.Greater(t, ttl.Seconds(), float64(0), "key %s should have positive TTL after open", key)
				assert.LessOrEqual(t, ttl.Seconds(), BatchTTL.Seconds(), "key %s TTL should be <= BatchTTL after open", key)
			}
		})

		t.Run("open push commit cycle", func(t *testing.T) {
			// Create batch
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push first job
			job1 := client.NewJob("TestJob", 1)
			job1.SetCustom("bid", bid)
			err = cl.Push(job1)
			require.NoError(t, err)

			// Commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Open batch
			_, err = cl.Generic("BATCH OPEN " + bid)
			require.NoError(t, err)

			// Push second job
			job2 := client.NewJob("TestJob", 2)
			job2.SetCustom("bid", bid)
			err = cl.Push(job2)
			require.NoError(t, err)

			// Re-commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Verify status shows total=2
			statusResult, err := cl.Generic("BATCH STATUS " + bid)
			require.NoError(t, err)

			var status client.BatchStatus
			err = json.Unmarshal([]byte(statusResult), &status)
			require.NoError(t, err)

			assert.Equal(t, int64(2), status.Total, "total should be 2 after open-push-commit cycle")
			assert.Equal(t, int64(2), status.Pending, "pending should be 2 (no jobs processed)")
		})

		t.Run("rejects non-existent batch", func(t *testing.T) {
			_, err := cl.Generic("BATCH OPEN non-existent-bid")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})

		t.Run("rejects missing BID", func(t *testing.T) {
			_, err := cl.Generic("BATCH OPEN")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing batch ID")
		})
	})
}

func TestBatchOpenAfterCallbackStarted(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		ctx := context.Background()
		rds := s.Manager().Redis()

		t.Run("rejects when success_st is CallbackEnqueued", func(t *testing.T) {
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			rds.Set(ctx, batchSuccessStateKey(bid), CallbackEnqueued, 0)

			_, err = cl.Generic("BATCH OPEN " + bid)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "callbacks have started")
		})

		t.Run("rejects when success_st is CallbackFinished", func(t *testing.T) {
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			rds.Set(ctx, batchSuccessStateKey(bid), CallbackFinished, 0)

			_, err = cl.Generic("BATCH OPEN " + bid)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "callbacks have started")
		})

		t.Run("rejects when complete_st is CallbackEnqueued", func(t *testing.T) {
			result, err := cl.Generic(`BATCH NEW {"complete":{"jobtype":"CompleteCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Set only complete_st, leave success_st empty
			rds.Set(ctx, batchCompleteStateKey(bid), CallbackEnqueued, 0)

			_, err = cl.Generic("BATCH OPEN " + bid)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "callbacks have started")
		})
	})
}

func TestBatchUnknownSubcommand(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		_, err := cl.Generic("BATCH UNKNOWN arg")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown BATCH subcommand")
	})
}

func TestBatchMissingSubcommand(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		_, err := cl.Generic("BATCH")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing subcommand")
	})
}

func TestBatchTTL(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		ctx := context.Background()
		redis := s.Manager().Redis()

		t.Run("uncommitted batch has TTL on all keys", func(t *testing.T) {
			// Create batch without committing
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Check all keys have TTL set
			keys := []string{
				batchMetaKey(bid),
				batchTotalKey(bid),
				batchPendingKey(bid),
				batchFailedKey(bid),
				batchCompleteStateKey(bid),
				batchSuccessStateKey(bid),
			}

			for _, key := range keys {
				ttl, err := redis.TTL(ctx, key).Result()
				require.NoError(t, err, "failed to get TTL for %s", key)
				assert.Greater(t, ttl.Seconds(), float64(0), "key %s should have positive TTL", key)
				assert.LessOrEqual(t, ttl.Seconds(), BatchTTL.Seconds(), "key %s TTL should be <= BatchTTL", key)
			}
		})

		t.Run("committed batch has no TTL", func(t *testing.T) {
			// Create and commit batch
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			bid := string(result)

			// Push a job so the batch doesn't immediately complete
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", bid)
			err = cl.Push(job)
			require.NoError(t, err)

			// Commit batch
			_, err = cl.Generic("BATCH COMMIT " + bid)
			require.NoError(t, err)

			// Check all keys have no TTL (-1 means no expiration)
			keys := []string{
				batchMetaKey(bid),
				batchTotalKey(bid),
				batchPendingKey(bid),
				batchFailedKey(bid),
				batchCompleteStateKey(bid),
				batchSuccessStateKey(bid),
			}

			for _, key := range keys {
				ttl, err := redis.TTL(ctx, key).Result()
				require.NoError(t, err, "failed to get TTL for %s", key)
				// TTL returns -1 nanosecond for keys with no expiration in go-redis
				assert.Equal(t, -1*time.Nanosecond, ttl, "key %s should have no TTL after commit", key)
			}
		})

		t.Run("children key has TTL before commit", func(t *testing.T) {
			// Create parent batch
			result, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"SuccessCallback"}}`)
			require.NoError(t, err)
			parentBid := string(result)

			// Create child batch with parent_bid
			result, err = cl.Generic(`BATCH NEW {"parent_bid":"` + parentBid + `","success":{"jobtype":"ChildCallback"}}`)
			require.NoError(t, err)
			_ = string(result) // childBid

			// Check children key has TTL
			ttl, err := redis.TTL(ctx, batchChildrenKey(parentBid)).Result()
			require.NoError(t, err)
			assert.Greater(t, ttl.Seconds(), float64(0), "children key should have positive TTL")

			// Push a job so parent doesn't immediately complete
			job := client.NewJob("TestJob", 1)
			job.SetCustom("bid", parentBid)
			err = cl.Push(job)
			require.NoError(t, err)

			// Commit parent batch
			_, err = cl.Generic("BATCH COMMIT " + parentBid)
			require.NoError(t, err)

			// Check children key has no TTL after commit
			ttl, err = redis.TTL(ctx, batchChildrenKey(parentBid)).Result()
			require.NoError(t, err)
			assert.Equal(t, -1*time.Nanosecond, ttl, "children key should have no TTL after commit")
		})
	})
}
