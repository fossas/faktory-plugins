package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/redis/go-redis/v9"
)

// Committed state constants
const (
	NotCommitted = "0"
	IsCommitted  = "1"
)

// BatchTTL is the TTL for uncommitted batch data.
// Batches that are not committed within this time will expire.
const BatchTTL = 30 * time.Minute

// Callback state constants
const (
	CallbackPending  = ""  // Callback not yet enqueued
	CallbackEnqueued = "1" // Callback job has been pushed to queue
	CallbackFinished = "2" // Callback job completed successfully
)

// ErrBatchNotFound is returned when a batch ID does not exist in Redis.
var ErrBatchNotFound = errors.New("batch not found")

// Redis key helpers

func batchKey(bid string) string {
	return fmt.Sprintf("batch:%s", bid)
}

func batchMetaKey(bid string) string {
	return batchKey(bid)
}

func batchTotalKey(bid string) string {
	return fmt.Sprintf("%s:total", batchKey(bid))
}

func batchPendingKey(bid string) string {
	return fmt.Sprintf("%s:pending", batchKey(bid))
}

func batchFailedKey(bid string) string {
	return fmt.Sprintf("%s:failed", batchKey(bid))
}

func batchCompleteStateKey(bid string) string {
	return fmt.Sprintf("%s:complete_st", batchKey(bid))
}

func batchSuccessStateKey(bid string) string {
	return fmt.Sprintf("%s:success_st", batchKey(bid))
}

func batchChildrenKey(bid string) string {
	return fmt.Sprintf("%s:children", batchKey(bid))
}

func batchCommittedSetKey() string {
	return "batches:committed"
}

// generateBid creates a new batch ID with the "b-" prefix
func generateBid() string {
	return "b-" + client.RandomJid()
}

// createBatch creates a new batch in Redis with the given definition
func createBatch(ctx context.Context, s *server.Server, batch *client.Batch) error {
	rds := s.Manager().Redis()

	// Serialize callback jobs to JSON
	var successJSON, completeJSON string
	if batch.Success != nil {
		data, err := json.Marshal(batch.Success)
		if err != nil {
			return fmt.Errorf("failed to marshal success callback: %w", err)
		}
		successJSON = string(data)
	}
	if batch.Complete != nil {
		data, err := json.Marshal(batch.Complete)
		if err != nil {
			return fmt.Errorf("failed to marshal complete callback: %w", err)
		}
		completeJSON = string(data)
	}

	// Use pipeline for atomic batch creation
	pipe := rds.TxPipeline()

	// Store batch metadata as hash
	pipe.HSet(ctx, batchMetaKey(batch.Bid), map[string]any{
		"parent_bid":  batch.ParentBid,
		"description": batch.Description,
		"success":     successJSON,
		"complete":    completeJSON,
		"created_at":  util.Nows(),
		"committed":   NotCommitted,
	})
	pipe.Expire(ctx, batchMetaKey(batch.Bid), BatchTTL)

	// Initialize counters with TTL
	pipe.Set(ctx, batchTotalKey(batch.Bid), 0, BatchTTL)
	pipe.Set(ctx, batchPendingKey(batch.Bid), 0, BatchTTL)
	pipe.Set(ctx, batchFailedKey(batch.Bid), 0, BatchTTL)

	// Initialize callback states with TTL
	pipe.Set(ctx, batchCompleteStateKey(batch.Bid), CallbackPending, BatchTTL)
	pipe.Set(ctx, batchSuccessStateKey(batch.Bid), CallbackPending, BatchTTL)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create batch in Redis: %w", err)
	}

	return nil
}

// batchExists checks if a batch exists in Redis
func batchExists(ctx context.Context, s *server.Server, bid string) (bool, error) {
	rds := s.Manager().Redis()
	exists, err := rds.Exists(ctx, batchMetaKey(bid)).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

// getBatch retrieves a batch definition from Redis
func getBatch(ctx context.Context, s *server.Server, bid string) (*client.Batch, error) {
	rds := s.Manager().Redis()

	data, err := rds.HGetAll(ctx, batchMetaKey(bid)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get batch: %w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("batch %s: %w", bid, ErrBatchNotFound)
	}

	batch := &client.Batch{
		Bid:         bid,
		ParentBid:   data["parent_bid"],
		Description: data["description"],
	}

	// Deserialize callback jobs
	if successJSON := data["success"]; successJSON != "" {
		var job client.Job
		if err := json.Unmarshal([]byte(successJSON), &job); err != nil {
			return nil, fmt.Errorf("failed to unmarshal success callback: %w", err)
		}
		batch.Success = &job
	}
	if completeJSON := data["complete"]; completeJSON != "" {
		var job client.Job
		if err := json.Unmarshal([]byte(completeJSON), &job); err != nil {
			return nil, fmt.Errorf("failed to unmarshal complete callback: %w", err)
		}
		batch.Complete = &job
	}

	return batch, nil
}

// getBatchStatus retrieves the current status of a batch
func getBatchStatus(ctx context.Context, s *server.Server, bid string) (*client.BatchStatus, error) {
	rds := s.Manager().Redis()

	// Get metadata
	data, err := rds.HGetAll(ctx, batchMetaKey(bid)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get batch metadata: %w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("batch %s: %w", bid, ErrBatchNotFound)
	}

	// Get counters and states
	pipe := rds.TxPipeline()
	totalCmd := pipe.Get(ctx, batchTotalKey(bid))
	pendingCmd := pipe.Get(ctx, batchPendingKey(bid))
	failedCmd := pipe.Get(ctx, batchFailedKey(bid))
	completeStCmd := pipe.Get(ctx, batchCompleteStateKey(bid))
	successStCmd := pipe.Get(ctx, batchSuccessStateKey(bid))

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get batch counters: %w", err)
	}

	total, err := strconv.ParseInt(totalCmd.Val(), 10, 64)
	if err != nil && totalCmd.Val() != "" {
		return nil, fmt.Errorf("failed to parse total counter for batch %s: %w", bid, err)
	}
	pending, err := strconv.ParseInt(pendingCmd.Val(), 10, 64)
	if err != nil && pendingCmd.Val() != "" {
		return nil, fmt.Errorf("failed to parse pending counter for batch %s: %w", bid, err)
	}
	failed, err := strconv.ParseInt(failedCmd.Val(), 10, 64)
	if err != nil && failedCmd.Val() != "" {
		return nil, fmt.Errorf("failed to parse failed counter for batch %s: %w", bid, err)
	}

	return &client.BatchStatus{
		Bid:           bid,
		ParentBid:     data["parent_bid"],
		Description:   data["description"],
		CreatedAt:     data["created_at"],
		CompleteState: completeStCmd.Val(),
		SuccessState:  successStCmd.Val(),
		Total:         total,
		Pending:       pending,
		Failed:        failed,
	}, nil
}

// isCommitted checks if a batch has been committed
func isCommitted(ctx context.Context, s *server.Server, bid string) (bool, error) {
	rds := s.Manager().Redis()
	committed, err := rds.HGet(ctx, batchMetaKey(bid), "committed").Result()
	if err != nil {
		return false, err
	}
	return committed == IsCommitted, nil
}

// setCommittedLua atomically persists all batch keys (removes TTL), sets the
// committed flag, and adds the batch to the committed set. This prevents
// partial failure where keys are persisted but the batch is never marked
// committed, which would leave permanently orphaned keys.
// KEYS[1..7] = batch keys to persist, KEYS[8] = committed set key
// ARGV[1] = bid
var setCommittedLua = redis.NewScript(`
	for i = 1, 7 do
		redis.call("PERSIST", KEYS[i])
	end
	redis.call("HSET", KEYS[1], "committed", "1")
	redis.call("SADD", KEYS[8], ARGV[1])
	return 1
`)

// setCommitted atomically marks a batch as committed, removes TTL from all
// keys, and adds it to the committed set.
func setCommitted(ctx context.Context, s *server.Server, bid string) error {
	rds := s.Manager().Redis()
	_, err := setCommittedLua.Run(ctx, rds,
		[]string{
			batchMetaKey(bid),          // KEYS[1]
			batchTotalKey(bid),         // KEYS[2]
			batchPendingKey(bid),       // KEYS[3]
			batchFailedKey(bid),        // KEYS[4]
			batchCompleteStateKey(bid), // KEYS[5]
			batchSuccessStateKey(bid),  // KEYS[6]
			batchChildrenKey(bid),      // KEYS[7]
			batchCommittedSetKey(),     // KEYS[8]
		},
		bid,
	).Result()
	if err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}
	return nil
}

// setUncommittedLua atomically checks that callbacks have not started,
// clears the committed flag, removes the batch from the committed set,
// and restores TTL on all batch keys. Returns 1 on success, 0 if
// callbacks have already started.
// KEYS[1] = batch meta, KEYS[2] = total, KEYS[3] = pending,
// KEYS[4] = failed, KEYS[5] = complete_st, KEYS[6] = success_st,
// KEYS[7] = children, KEYS[8] = committed set
// ARGV[1] = bid, ARGV[2] = TTL in seconds
var setUncommittedLua = redis.NewScript(`
	local complete_st = redis.call("GET", KEYS[5])
	local success_st = redis.call("GET", KEYS[6])
	if (complete_st ~= false and complete_st ~= "") or (success_st ~= false and success_st ~= "") then
		return 0
	end
	redis.call("HSET", KEYS[1], "committed", "0")
	redis.call("SREM", KEYS[8], ARGV[1])
	for i = 1, 7 do
		redis.call("EXPIRE", KEYS[i], ARGV[2])
	end
	return 1
`)

// setUncommitted atomically checks callbacks are pending, clears the committed
// flag, removes the batch from the committed set, and restores TTL on all keys.
func setUncommitted(ctx context.Context, s *server.Server, bid string) error {
	rds := s.Manager().Redis()
	result, err := setUncommittedLua.Run(ctx, rds,
		[]string{
			batchMetaKey(bid),          // KEYS[1]
			batchTotalKey(bid),         // KEYS[2]
			batchPendingKey(bid),       // KEYS[3]
			batchFailedKey(bid),        // KEYS[4]
			batchCompleteStateKey(bid), // KEYS[5]
			batchSuccessStateKey(bid),  // KEYS[6]
			batchChildrenKey(bid),      // KEYS[7]
			batchCommittedSetKey(),     // KEYS[8]
		},
		bid,
		int(BatchTTL.Seconds()),
	).Int()
	if err != nil {
		return fmt.Errorf("failed to uncommit batch: %w", err)
	}
	if result == 0 {
		return fmt.Errorf("cannot reopen batch after callbacks have started")
	}
	return nil
}

// deleteBatch removes all Redis keys associated with a batch
func deleteBatch(ctx context.Context, s *server.Server, bid string) error {
	rds := s.Manager().Redis()
	pipe := rds.TxPipeline()

	keys := []string{
		batchMetaKey(bid),
		batchTotalKey(bid),
		batchPendingKey(bid),
		batchFailedKey(bid),
		batchCompleteStateKey(bid),
		batchSuccessStateKey(bid),
		batchChildrenKey(bid),
		batchCompleteStateKey(bid) + ":lock",
		batchSuccessStateKey(bid) + ":lock",
	}
	pipe.Del(ctx, keys...)
	pipe.SRem(ctx, batchCommittedSetKey(), bid)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete batch: %w", err)
	}

	util.Debugf("Deleted batch %s", bid)
	return nil
}
