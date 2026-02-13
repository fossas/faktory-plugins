package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
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
	redis := s.Manager().Redis()

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
	pipe := redis.Pipeline()

	// Store batch metadata as hash
	pipe.HSet(ctx, batchMetaKey(batch.Bid), map[string]interface{}{
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
	redis := s.Manager().Redis()
	exists, err := redis.Exists(ctx, batchMetaKey(bid)).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

// getBatch retrieves a batch definition from Redis
func getBatch(ctx context.Context, s *server.Server, bid string) (*client.Batch, error) {
	redis := s.Manager().Redis()

	data, err := redis.HGetAll(ctx, batchMetaKey(bid)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get batch: %w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("batch %s not found", bid)
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
	redis := s.Manager().Redis()

	// Get metadata
	data, err := redis.HGetAll(ctx, batchMetaKey(bid)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get batch metadata: %w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("batch %s not found", bid)
	}

	// Get counters and states
	pipe := redis.Pipeline()
	totalCmd := pipe.Get(ctx, batchTotalKey(bid))
	pendingCmd := pipe.Get(ctx, batchPendingKey(bid))
	failedCmd := pipe.Get(ctx, batchFailedKey(bid))
	completeStCmd := pipe.Get(ctx, batchCompleteStateKey(bid))
	successStCmd := pipe.Get(ctx, batchSuccessStateKey(bid))

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch counters: %w", err)
	}

	total, _ := strconv.ParseInt(totalCmd.Val(), 10, 64)
	pending, _ := strconv.ParseInt(pendingCmd.Val(), 10, 64)
	failed, _ := strconv.ParseInt(failedCmd.Val(), 10, 64)

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
	redis := s.Manager().Redis()
	committed, err := redis.HGet(ctx, batchMetaKey(bid), "committed").Result()
	if err != nil {
		return false, err
	}
	return committed == IsCommitted, nil
}

// persistBatch removes TTL from all batch keys, making them permanent.
// This is called when a batch is committed.
func persistBatch(ctx context.Context, s *server.Server, bid string) error {
	redis := s.Manager().Redis()

	keys := []string{
		batchMetaKey(bid),
		batchTotalKey(bid),
		batchPendingKey(bid),
		batchFailedKey(bid),
		batchCompleteStateKey(bid),
		batchSuccessStateKey(bid),
		batchChildrenKey(bid),
	}

	pipe := redis.Pipeline()
	for _, key := range keys {
		pipe.Persist(ctx, key)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to persist batch keys: %w", err)
	}

	return nil
}

// setCommitted marks a batch as committed and removes TTL from all keys
func setCommitted(ctx context.Context, s *server.Server, bid string) error {
	// Remove TTL from all batch keys
	if err := persistBatch(ctx, s, bid); err != nil {
		return err
	}

	redis := s.Manager().Redis()
	if err := redis.HSet(ctx, batchMetaKey(bid), "committed", IsCommitted).Err(); err != nil {
		return err
	}
	return redis.SAdd(ctx, batchCommittedSetKey(), bid).Err()
}

// deleteBatch removes all Redis keys associated with a batch
func deleteBatch(ctx context.Context, s *server.Server, bid string) error {
	redis := s.Manager().Redis()

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

	_, err := redis.Del(ctx, keys...).Result()
	if err != nil {
		return fmt.Errorf("failed to delete batch: %w", err)
	}

	redis.SRem(ctx, batchCommittedSetKey(), bid)

	util.Debugf("Deleted batch %s", bid)
	return nil
}
