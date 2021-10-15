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

type BatchSubSystem struct {
	Server  *server.Server
	Batches map[string]*Batch
	mu      sync.Mutex
	Fetcher manager.Fetcher
}

type Lifecycle struct {
	BatchSubSystem *BatchSubSystem
}

type NewBatchRequest struct {
	ParentBid   string      `json:"parent_bid,omitempty"`
	Description string      `json:"description,omitempty"`
	Success     *client.Job `json:"success,omitempty"`
	Complete    *client.Job `json:"complete,omitempty"`
}

func Subsystem() *Lifecycle {
	return &Lifecycle{}
}

func (b *BatchSubSystem) Start(s *server.Server) error {
	b.Server = s
	b.mu = sync.Mutex{}
	b.Batches = make(map[string]*Batch)
	b.Fetcher = manager.BasicFetcher(s.Manager().Redis())
	if err := b.loadExistingBatches(); err != nil {
		util.Warnf("loading existing batches: %v", v)
	}
	b.addCommands()
	b.addMiddleware()
	util.Info("Loaded batching plugin")
	return nil
}

func (b *BatchSubSystem) Name() string {
	return "Batch"
}

func (b *BatchSubSystem) Reload(s *server.Server) error {
	return nil
}

func (b *BatchSubSystem) Shutdown(s *server.Server) error {
	return nil
}

func (b *BatchSubSystem) addCommands() {
	server.CommandSet["BATCH"] = func(c *server.Connection, s *server.Server, cmd string) {
		util.Info(fmt.Sprintf("cmd: %s", cmd))
		qs := strings.Split(cmd, " ")[1:]

		switch batchOperation := qs[0]; batchOperation {
		case "NEW":
			var batchRequest NewBatchRequest
			if err := json.Unmarshal([]byte(qs[1]), &batchRequest); err != nil {
				c.Error(cmd, fmt.Errorf("Invalid JSON data: %v", err))
			}

			batchId := fmt.Sprintf("b-%s", util.RandomJid())

			success := ""
			if batchRequest.Success != nil {
				successData, err := json.Marshal(batchRequest.Success)
				if err != nil {
					c.Error(cmd, fmt.Errorf("Invalid Success job"))
				}
				success = string(successData)
			}

			complete := ""
			if batchRequest.Complete != nil {
				completeData, err := json.Marshal(batchRequest.Complete)
				if err != nil {
					c.Error(cmd, fmt.Errorf("Invalid Complete job"))
				}
				complete = string(completeData)
			}

			meta := b.newBatchMeta(batchRequest.Description, batchRequest.ParentBid, success, complete)
			batch, err := b.newBatch(batchId, meta)

			if err != nil {
				c.Error(cmd, fmt.Errorf("Unable to create batch: %v", err))
				return
			}

			c.Result([]byte(batch.Id))
		case "OPEN":
			batchId := qs[1]

			// worker ids are usually only associated with workers
			// in order for a client to submit a job to a batch it must pass wid to the payload when submitting HELO

			// to retrieve the worker id for this request
			// we must access client which is a private field of Connection
			// use reflection in order to get the worker id that is requesting to open the batch
			connection := reflect.ValueOf(*c)
			client := connection.FieldByName("client").Elem()

			wid := client.FieldByName("Wid").String()
			if wid == "" {
				c.Error(cmd, fmt.Errorf("Batches can only be opened from a client with wid set"))
			}

			batch, err := b.getBatch(batchId)
			if err != nil {
				c.Error(cmd, fmt.Errorf("Cannot find batch: %v", err))
			}

			if !batch.hasWorker(wid) {
				c.Error(cmd, fmt.Errorf("This worker is not working on a job in the requested batch"))
			}

			if err := batch.open(); err != nil {
				c.Error(cmd, fmt.Errorf("Cannot open batch: %v", err))
			}

			c.Ok()
		case "COMMIT":
			batchId := qs[1]

			batch, err := b.getBatch(batchId)
			if err != nil {
				c.Error(cmd, fmt.Errorf("Cannot find batch: %v", err))
			}

			if err := batch.commit(); err != nil {
				c.Error(cmd, fmt.Errorf("Cannot commit batch: %v", err))
			}
			c.Ok()

		case "STATUS":
			batchId := qs[1]
			batch, err := b.getBatch(batchId)
			if err != nil {
				c.Error(cmd, fmt.Errorf("Cannot find batch: %v", err))
			}
			data, err := json.Marshal(map[string]interface{}{
				"bid":         batchId,
				"total":       batch.Meta.Total,
				"pending":     batch.Meta.Total - batch.Meta.Failed - batch.Meta.Succeeded,
				"description": batch.Meta.Description,
				"created_at":  batch.Meta.CreatedAt,
			})
			if err != nil {
				c.Error(cmd, fmt.Errorf("Unable to marshal batch data: %v", err))
			}
			c.Result([]byte(data))
		default:
			c.Error(cmd, fmt.Errorf("Invalid BATCH operation %s", qs[0]))
		}

	}
}

/*
These are the steps within middleware

push:
	- check for job.custom.bid
	- check batch is open

	- push event to update bid data:
		- total + 1
		- pending + 1

ack:
	- check for job.custom.bid
	- check batch is open
	- event to update bid data <- do checks in there:
		- success + 1
	- event to see if success job should be pushed
	- event to see if completed job should be pushed

fail:
	- check for job.custom.bid
	- check batch is open
	- event to update bid data:
		- failed + 1
	- event to see if completed job should be pushed
*/

func (b *BatchSubSystem) addMiddleware() {
	// we have to set a custom fetcher in order to set the worker id for a job
	b.Server.Manager().SetFetcher(b)
	b.Server.Manager().AddMiddleware("push", b.pushMiddleware)
	b.Server.Manager().AddMiddleware("fetch", b.fetchMiddleware)
	b.Server.Manager().AddMiddleware("ack", b.handleJobFinished(true))
	b.Server.Manager().AddMiddleware("fail", b.handleJobFinished(false))
}

func (b *BatchSubSystem) getBatchFromInterface(batchId interface{}) (*Batch, error) {
	bid, ok := batchId.(string)
	if !ok {
		return nil, errors.New("Invalid custom bid value")
	}
	batch, err := b.getBatch(bid)
	if err != nil {
		return nil, fmt.Errorf("Unable to get batch: %s", bid)
	}
	return batch, nil
}

func (b *BatchSubSystem) pushMiddleware(next func() error, ctx manager.Context) error {
	if bid, ok := ctx.Job().GetCustom("bid"); ok {
		batch, err := b.getBatchFromInterface(bid)
		if err != nil {
			return fmt.Errorf("Unable to retrieve batch %s", bid)
		}
		batch.jobQueued(ctx.Job().Jid)
		util.Infof("Added %s to batch %s", ctx.Job().Jid, batch.Id)
	}
	return next()
}

func (b *BatchSubSystem) Fetch(ctx context.Context, wid string, queues ...string) (manager.Lease, error) {
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

func (b *BatchSubSystem) fetchMiddleware(next func() error, ctx manager.Context) error {
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

func (b *BatchSubSystem) handleJobFinished(success bool) func(next func() error, ctx manager.Context) error {
	return func(next func() error, ctx manager.Context) error {
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

func (b *BatchSubSystem) loadExistingBatches() error {
	vals, err := b.Server.Manager().Redis().SMembers("batches").Result()
	if err != nil {
		return fmt.Errorf("retrieve batches: %v", err)
	}
	for idx := range vals {
		batch, err := b.newBatch(vals[idx], &BatchMeta{})
		if err != nil {
			util.Warnf("create batch: %v", err)
			continue
		}
		b.Batches[vals[idx]] = batch
	}
	return nil
}
func (b *BatchSubSystem) newBatchMeta(description string, parentBid string, success string, complete string) *BatchMeta {
	return &BatchMeta{
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Total:       0,
		Succeeded:   0,
		Failed:      0,
		Description: description,
		ParentBid:   parentBid,
		SuccessJob:  success,
		CompleteJob: complete,
	}
}

func (b *BatchSubSystem) newBatch(batchId string, meta *BatchMeta) (*Batch, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	batch := &Batch{
		Id:          batchId,
		BatchKey:    fmt.Sprintf("batch-%s", batchId),
		JobsKey:     fmt.Sprintf("jobs-%s", batchId),
		ChildrenKey: fmt.Sprintf("child-%s", batchId),
		MetaKey:     fmt.Sprintf("meta-%s", batchId),
		Meta:        meta,
		rclient:     b.Server.Manager().Redis(),
		mu:          sync.Mutex{},
		Workers:     make(map[string]string),
		Server:      b.Server,
	}
	if err := batch.init(); err != nil {
		return nil, fmt.Errorf("Unable to initialize batch: %v", err)
	}
	if err := b.Server.Manager().Redis().SAdd("batches", batchId).Err(); err != nil {
		return nil, fmt.Errorf("Unable to store batch: %v", err)
	}

	b.Server.Manager().Redis().SetNX(batch.BatchKey, batch.Id, time.Duration(2*time.Hour))

	b.Batches[batchId] = batch

	return batch, nil

}

func (b *BatchSubSystem) getBatch(batchId string) (*Batch, error) {
	if batchId == "" {
		return nil, fmt.Errorf("batchId cannot be blank")
	}

	batch, ok := b.Batches[batchId]

	if !ok {
		return nil, fmt.Errorf("No batch found")
	}

	if err := b.Server.Manager().Redis().Get(batch.BatchKey).Err(); err != nil {
		b.removeBatch(batch)
		return nil, fmt.Errorf("Batch was not commited within 2 hours.")
	}

	return batch, nil
}

func (b *BatchSubSystem) removeBatch(batch *Batch) {
	if err := b.Server.Manager().Redis().SRem("batches", batch.Id).Err(); err != nil {
		util.Warnf("Unable to remove batch: %s, %v", batch.Id, err)
	}
	delete(b.Batches, batch.Id)
	batch.remove()
	batch = nil
}
