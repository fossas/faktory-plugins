package batch

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/contribsys/faktory/client"
	"strings"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

// NewBatchRequest structure for a new batch request
// Success and Complete are jobs to be queued
// once the batch has been committed and all jobs processed
type NewBatchRequest struct {
	//	ParentBid   string      `json:"parent_bid,omitempty"`
	Description      string      `json:"description,omitempty"`
	Success          *client.Job `json:"success,omitempty"`
	Complete         *client.Job `json:"complete,omitempty"`
	ChildSearchDepth *int        `json:"child_search_depth,omitempty"`
}

func (b *BatchSubsystem) batchCommand(c *server.Connection, s *server.Server, cmd string) {
	parts := strings.SplitN(cmd, " ", 3)[1:]
	if len(parts) < 2 {
		_ = c.Error(cmd, errors.New("invalid BATCH command"))
		return
	}

	switch batchOperation := parts[0]; batchOperation {
	case "NEW":
		var batchRequest NewBatchRequest
		if err := json.Unmarshal([]byte(parts[1]), &batchRequest); err != nil {
			_ = c.Error(cmd, fmt.Errorf("invalid JSON data: %v", err))
			return
		}

		batchId := fmt.Sprintf("b-%s", util.RandomJid())

		success := ""
		if batchRequest.Success != nil {
			successData, err := json.Marshal(batchRequest.Success)
			if err != nil {
				_ = c.Error(cmd, fmt.Errorf("invalid Success job"))
				return
			}
			success = string(successData)
		}

		complete := ""
		if batchRequest.Complete != nil {
			completeData, err := json.Marshal(batchRequest.Complete)
			if err != nil {
				_ = c.Error(cmd, fmt.Errorf("invalid Complete job"))
				return
			}
			complete = string(completeData)
		}

		meta := b.newBatchMeta(batchRequest.Description, success, complete, batchRequest.ChildSearchDepth)
		batch, err := b.newBatch(batchId, meta)

		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("unable to create batch: %v", err))
			return
		}

		_ = c.Result([]byte(batch.Id))
		return
	case "OPEN":
		batchId := parts[1]

		batch, err := b.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot get batch: %v", err))
			return
		}

		if batch.areBatchJobsCompleted() {
			_ = c.Error(cmd, errors.New("batch has already finished"))
			return
		}

		if batch.Meta.Committed {
			if err := batch.open(); err != nil {
				_ = c.Error(cmd, fmt.Errorf("cannot open batch: %v", err))
				return
			}
		}

		_ = c.Result([]byte(batch.Id))
		return
	case "COMMIT":
		batchId := parts[1]
		if batchId == "" {
			_ = c.Error(cmd, errors.New("bid is required"))
			return
		}
		batch, err := b.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot get batch: %v", err))
			return
		}

		if err := batch.commit(); err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot commit batch: %v", err))
			return
		}
		_ = c.Ok()
		return
	case "CHILD":
		// BATCH CHILD batchId childId
		subParts := strings.Split(parts[1], " ")
		if len(subParts) != 2 {
			_ = c.Error(cmd, fmt.Errorf("must include child and parent Bid: %s", parts))
			return
		}
		batchId := subParts[0]
		childBatchId := subParts[1]

		batch, err := b.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot get batch: %v", err))
			return
		}

		if batch.Meta.Committed {
			_ = c.Error(cmd, errors.New("batch has already been committed, child batches cannot be added"))
			return
		}

		if batch.areBatchJobsCompleted() {
			_ = c.Error(cmd, errors.New("batch has already finished"))
			return
		}

		childBatch, err := b.getBatch(childBatchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot get child batch: %v", err))
			return
		}

		if err := batch.addChild(childBatch); err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot add child (%s) to batch (%s): %v", childBatchId, batchId, err))
		}

		_ = c.Ok()
		return
	case "STATUS":
		batchId := parts[1]
		batch, err := b.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot find batch: %v", err))
			return
		}
		data, err := json.Marshal(map[string]interface{}{
			"bid":          batchId,
			"total":        batch.Meta.Total,
			"pending":      batch.Meta.Pending,
			"description":  batch.Meta.Description,
			"created_at":   batch.Meta.CreatedAt,
			"completed_st": CallbackJobPending,
			"success_st":   CallbackJobPending,
		})
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("unable to marshal batch data: %v", err))
			return
		}
		_ = c.Result([]byte(data))
		return
	default:
		_ = c.Error(cmd, fmt.Errorf("invalid BATCH operation %s", parts[0]))
	}
}
