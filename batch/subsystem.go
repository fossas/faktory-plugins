package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"

	"github.com/contribsys/faktory/util"
)

// BatchSubsystem this enables jobs to be grouped into a batch
// the implementation follows the spec here: https://github.com/contribsys/faktory/wiki/Ent-Batches
// With the exception of child batches, which are not implemented
// for a client to re-open a batch it must have a WID (worker id) applied to it
// that worker must be processing a job within the batch
type BatchSubsystem struct {
	Server  *server.Server
	Batches map[string]*batch
	mu      sync.Mutex
	Fetcher manager.Fetcher
}

// structure for a new batch request
// Succecss and Complete are jobs to be queued
// once the batch has been commited and all jobs procssed
type NewBatchRequest struct {
	//	ParentBid   string      `json:"parent_bid,omitempty"`
	Description string      `json:"description,omitempty"`
	Success     *client.Job `json:"success,omitempty"`
	Complete    *client.Job `json:"complete,omitempty"`
}

// Starts the batch subsystem
func (b *BatchSubsystem) Start(s *server.Server) error {
	b.Server = s
	b.mu = sync.Mutex{}
	b.Batches = make(map[string]*batch)
	b.Fetcher = manager.BasicFetcher(s.Manager().Redis())
	if err := b.loadExistingBatches(); err != nil {
		util.Warnf("loading existing batches: %v", err)
	}
	b.addCommands()
	b.addMiddleware()
	util.Info("Loaded batching plugin")
	return nil
}

// name of the plugin
func (b *BatchSubsystem) Name() string {
	return "Batch"
}

// Reload does not do anything but is required by Faktory for subsystems
func (b *BatchSubsystem) Reload(s *server.Server) error {
	return nil
}

// Shutdown does not do anything but is required by Faktory for subystems
func (b *BatchSubsystem) Shutdown(s *server.Server) error {
	return nil
}

func (b *BatchSubsystem) addCommands() {
	server.CommandSet["BATCH"] = func(c *server.Connection, s *server.Server, cmd string) {
		util.Info(fmt.Sprintf("cmd: %s", cmd))
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
}

func (b *BatchSubsystem) addMiddleware() {
	// we have to set a custom fetcher in order to set the worker id for a job
	b.Server.Manager().SetFetcher(b)
	b.Server.Manager().AddMiddleware("push", b.pushMiddleware)
	b.Server.Manager().AddMiddleware("fetch", b.fetchMiddleware)
	b.Server.Manager().AddMiddleware("ack", b.handleJobFinished(true))
	b.Server.Manager().AddMiddleware("fail", b.handleJobFinished(false))
}

func (b *BatchSubsystem) getBatchFromInterface(batchId interface{}) (*batch, error) {
	bid, ok := batchId.(string)
	if !ok {
		return nil, errors.New("Invalid custom bid value")
	}
	batch, err := b.getBatch(bid)
	if err != nil {
		util.Warnf("Unable to retrieve batch: %v", err)
		return nil, fmt.Errorf("Unable to get batch: %s", bid)
	}
	return batch, nil
}

func (b *BatchSubsystem) pushMiddleware(next func() error, ctx manager.Context) error {
	if bid, ok := ctx.Job().GetCustom("bid"); ok {
		batch, err := b.getBatchFromInterface(bid)
		if err != nil {
			return fmt.Errorf("Unable to get batch %s", bid)
		}
		if err := batch.jobQueued(ctx.Job().Jid); err != nil {
			util.Warnf("Unable to add batch %v", err)
			return fmt.Errorf("Unable to add job %s to batch %s", ctx.Job().Jid, bid)
		}
		util.Infof("Added %s to batch %s", ctx.Job().Jid, batch.Id)
	}
	return next()
}

func (b *BatchSubsystem) Fetch(ctx context.Context, wid string, queues ...string) (manager.Lease, error) {
	lease, err := b.Fetcher.Fetch(ctx, wid, queues...)
	if err == nil && lease != manager.Nothing {
		job, err := lease.Job()
		if err == nil && job != nil {
			if bid, ok := job.GetCustom("bid"); ok {
				batch, err := b.getBatchFromInterface(bid)
				if err != nil {
					return nil, fmt.Errorf("Unable to retrieve batch %s", bid)
				}
				batch.setWorkerForJid(job.Jid, wid)
				util.Infof("Added worker %s for job %s to %s", wid, job.Jid, batch.Id)
			}
		}

	}
	return lease, err
}

func (b *BatchSubsystem) fetchMiddleware(next func() error, ctx manager.Context) error {
	middleware_err := next() // runs the rest of the middleware
	if bid, ok := ctx.Job().GetCustom("bid"); ok {
		batch, err := b.getBatchFromInterface(bid)
		if err != nil {
			return fmt.Errorf("Unable to retrieve batch %s", bid)
		}
		if middleware_err != nil {
			// clear the worker id for a job here since we set the worker id in the custom fetcher
			batch.removeWorkerForJid(ctx.Job().Jid)
		}
	}

	return middleware_err
}

func (b *BatchSubsystem) handleJobFinished(success bool) func(next func() error, ctx manager.Context) error {
	return func(next func() error, ctx manager.Context) error {
		if success {
			// check if this is a success / complete job from batch
			if bid, ok := ctx.Job().GetCustom("_bid"); ok {
				batch, err := b.getBatchFromInterface(bid)
				if err != nil {
					util.Warnf("Unable to retrieve batch %s: %v", bid, err)
					return next()
				}
				cb, ok := ctx.Job().GetCustom("_cb")
				if !ok {
					util.Warnf("Batch (%s) callback job (%s) does not have _cb specified", bid, ctx.Job().Type)
					return next()
				}
				callbackType, ok := cb.(string)
				if !ok {
					util.Warnf("Error converting callback job type %s", cb)
					return next()
				}
				if err := batch.callbackJobSucceded(callbackType); err != nil {
					util.Warnf("Unable to update batch")
				}
				return next()
			}
		}
		if bid, ok := ctx.Job().GetCustom("bid"); ok {
			batch, err := b.getBatchFromInterface(bid)
			if err != nil {
				return fmt.Errorf("Unable to retrieve batch %s", bid)
			}

			status := "succeeded"
			if !success {
				status = "failed"
			}
			util.Infof("Job %s (worker %s) %s for batch %s", ctx.Job().Jid, ctx.Reservation().Wid, status, batch.Id)
			batch.removeWorkerForJid(ctx.Job().Jid)
			if err := batch.jobFinished(ctx.Job().Jid, success); err != nil {
				util.Warnf("error processing finished job for batch %v", err)
				return fmt.Errorf("Unable to process finished job %s for batch %s", ctx.Job().Jid, batch.Id)
			}

		}
		return next()
	}
}

func (b *BatchSubsystem) loadExistingBatches() error {
	vals, err := b.Server.Manager().Redis().SMembers("batches").Result()
	if err != nil {
		return fmt.Errorf("retrieve batches: %v", err)
	}
	for idx := range vals {
		batch, err := b.newBatch(vals[idx], &batchMeta{})
		if err != nil {
			util.Warnf("create batch: %v", err)
			continue
		}
		b.Batches[vals[idx]] = batch
	}
	return nil
}
func (b *BatchSubsystem) newBatchMeta(description string, success string, complete string) *batchMeta {
	return &batchMeta{
		CreatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		Total:            0,
		Succeeded:        0,
		Failed:           0,
		Description:      description,
		SuccessJob:       success,
		CompleteJob:      complete,
		SuccessJobState:  CallbackJobPending,
		CompleteJobState: CallbackJobPending,
	}
}

func (b *BatchSubsystem) newBatch(batchId string, meta *batchMeta) (*batch, error) {
	batch := &batch{
		Id:       batchId,
		BatchKey: fmt.Sprintf("batch-%s", batchId),
		JobsKey:  fmt.Sprintf("jobs-%s", batchId),
		MetaKey:  fmt.Sprintf("meta-%s", batchId),
		Meta:     meta,
		rclient:  b.Server.Manager().Redis(),
		mu:       sync.Mutex{},
		Workers:  make(map[string]string),
		Server:   b.Server,
	}
	if err := batch.init(); err != nil {
		return nil, fmt.Errorf("initialize batch: %v", err)
	}

	b.Batches[batchId] = batch

	return batch, nil
}

func (b *BatchSubsystem) getBatch(batchId string) (*batch, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if batchId == "" {
		return nil, fmt.Errorf("batchId cannot be blank")
	}

	batch, ok := b.Batches[batchId]

	if !ok {
		return nil, fmt.Errorf("No batch found")
	}

	exists, err := b.Server.Manager().Redis().Exists(batch.BatchKey).Result()
	if err != nil {
		util.Warnf("Cannot confirm batch exists: %v", err)
		return nil, fmt.Errorf("Unable to check if batch has timed out")
	}
	if exists == 0 {
		b.removeBatch(batch)
		return nil, fmt.Errorf("Batch was not commited within 2 hours")
	}

	return batch, nil
}

func (b *BatchSubsystem) removeBatch(batch *batch) {
	if err := batch.remove(); err != nil {
		util.Warnf("Unable to remove batch: %v", err)
	}
	delete(b.Batches, batch.Id)

	batch = nil
}
