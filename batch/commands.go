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

		if len(success) == 0 && len(complete) == 0 {
			_ = c.Error(cmd, fmt.Errorf("success and/or a complete job callback must be included in batch creation"))
			return
		}

		meta := b.batchManager.newBatchMeta(batchRequest.Description, success, complete, batchRequest.ChildSearchDepth)
		batch, err := b.batchManager.newBatch(batchId, meta)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("unable to create batch: %v", err))
			return
		}

		_ = c.Result([]byte(batch.Id))
		return
	case "OPEN":
		batchId := parts[1]

		b.batchManager.lockBatch(batchId)
		defer b.batchManager.unlockBatch(batchId)
		batch, err := b.batchManager.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot get batch: %v", err))
			return
		}

		if b.batchManager.areBatchJobsCompleted(batch) {
			_ = c.Error(cmd, errors.New("batch has already finished"))
			return
		}

		if batch.Meta.Committed {
			if err := b.batchManager.open(batch); err != nil {
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
		b.batchManager.lockBatch(batchId)
		defer b.batchManager.unlockBatch(batchId)
		batch, err := b.batchManager.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot get batch: %v", err))
			return
		}

		if err := b.batchManager.commit(batch); err != nil {
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
		if childBatchId == batchId {
			_ = c.Error(cmd, fmt.Errorf("child batch and parent batch cannot be the same value"))
			return
		}
		b.batchManager.lockBatch(batchId)
		defer b.batchManager.unlockBatch(batchId)
		batch, err := b.batchManager.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot get batch: %v", err))
			return
		}
		opened := false
		if batch.Meta.Committed {
			// open will check if the batch has already finished
			if err := b.batchManager.open(batch); err != nil {
				_ = c.Error(cmd, errors.New("cannot open committed batch"))
				return
			}
			opened = true
		}

		b.batchManager.lockBatch(childBatchId)
		defer b.batchManager.unlockBatch(childBatchId)
		childBatch, err := b.batchManager.getBatch(childBatchId)
		ok := true
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot get child batch: %v", err))
			ok = false
		} else if err := b.batchManager.addChild(batch, childBatch); err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot add child (%s) to batch (%s): %v", childBatchId, batchId, err))
			ok = false
		}
		// ensure batch is committed if it was opened
		if opened {
			if err := b.batchManager.commit(batch); err != nil {
				_ = c.Error(cmd, errors.New("cannot commit batch"))
				return
			}
		}
		if ok {
			_ = c.Ok()
		}
		return
	case "STATUS":
		batchId := parts[1]
		b.batchManager.lockBatch(batchId)
		defer b.batchManager.unlockBatch(batchId)
		batch, err := b.batchManager.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("cannot find batch: %v", err))
			return
		}
		childSearchDepth := batch.Meta.ChildSearchDepth
		if childSearchDepth == nil {
			childSearchDepth = &b.Options.ChildSearchDepth
		}
		data, err := json.Marshal(map[string]interface{}{
			"bid":                batchId,
			"total":              batch.Meta.Total,
			"pending":            batch.Meta.Pending,
			"description":        batch.Meta.Description,
			"created_at":         batch.Meta.CreatedAt,
			"completed_st":       CallbackJobPending,
			"success_st":         CallbackJobPending,
			"child_search_depth": childSearchDepth,
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
