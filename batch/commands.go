package batch

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

func (b *BatchSubsystem) batchCommand(c *server.Connection, s *server.Server, cmd string) {
	parts := strings.SplitN(cmd, " ", 3)[1:]
	if len(parts) < 2 {
		c.Error(cmd, errors.New("Invalid BATCH command"))
		return
	}

	switch batchOperation := parts[0]; batchOperation {
	case "NEW":
		var batchRequest NewBatchRequest
		if err := json.Unmarshal([]byte(parts[1]), &batchRequest); err != nil {
			_ = c.Error(cmd, fmt.Errorf("Invalid JSON data: %v", err))
			return
		}

		batchId := fmt.Sprintf("b-%s", util.RandomJid())

		success := ""
		if batchRequest.Success != nil {
			successData, err := json.Marshal(batchRequest.Success)
			if err != nil {
				_ = c.Error(cmd, fmt.Errorf("Invalid Success job"))
				return
			}
			success = string(successData)
		}

		complete := ""
		if batchRequest.Complete != nil {
			completeData, err := json.Marshal(batchRequest.Complete)
			if err != nil {
				_ = c.Error(cmd, fmt.Errorf("Invalid Complete job"))
				return
			}
			complete = string(completeData)
		}

		meta := b.newBatchMeta(batchRequest.Description, success, complete)
		batch, err := b.newBatch(batchId, meta)

		if err != nil {
			c.Error(cmd, fmt.Errorf("Unable to create batch: %v", err))
			return
		}

		_ = c.Result([]byte(batch.Id))
		return
	case "OPEN":
		batchId := parts[1]

		// worker ids are usually only associated with workers
		// in order for a client to submit a job to a batch it must pass wid to the payload when submitting HELO

		// to retrieve the worker id for this request
		// we must access client which is a private field of Connection
		// use reflection in order to get the worker id that is requesting to open the batch
		connection := reflect.ValueOf(*c)
		client := connection.FieldByName("client").Elem()

		wid := client.FieldByName("Wid").String()
		if wid == "" {
			_ = c.Error(cmd, fmt.Errorf("Batches can only be opened from a client with wid set"))
			return
		}

		batch, err := b.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("Cannot get batch: %v", err))
			return
		}

		if batch.isBatchDone() {
			_ = c.Error(cmd, errors.New("Batch has already finished"))
			return
		}

		if !batch.hasWorker(wid) {
			_ = c.Error(cmd, fmt.Errorf("This worker is not working on a job in the requested batch"))
			return
		}

		if err := batch.open(); err != nil {
			_ = c.Error(cmd, fmt.Errorf("Cannot open batch: %v", err))
			return
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
			_ = c.Error(cmd, fmt.Errorf("Cannot get batch: %v", err))
			return
		}

		if err := batch.commit(); err != nil {
			_ = c.Error(cmd, fmt.Errorf("Cannot commit batch: %v", err))
			return
		}
		_ = c.Ok()
		return
	case "STATUS":
		batchId := parts[1]
		batch, err := b.getBatch(batchId)
		if err != nil {
			_ = c.Error(cmd, fmt.Errorf("Cannot find batch: %v", err))
			return
		}
		data, err := json.Marshal(map[string]interface{}{
			"bid":          batchId,
			"total":        batch.Meta.Total,
			"pending":      batch.Meta.Total - batch.Meta.Failed - batch.Meta.Succeeded,
			"description":  batch.Meta.Description,
			"created_at":   batch.Meta.CreatedAt,
			"completed_st": CallbackJobPending,
			"success_st":   CallbackJobPending,
		})
		if err != nil {
			c.Error(cmd, fmt.Errorf("Unable to marshal batch data: %v", err))
			return
		}
		_ = c.Result([]byte(data))
		return
	default:
		_ = c.Error(cmd, fmt.Errorf("Invalid BATCH operation %s", parts[0]))
	}
}
