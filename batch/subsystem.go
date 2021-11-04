package batch

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

// BatchSubsystem enables jobs to be grouped into a batch
// the implementation follows the spec here: https://github.com/contribsys/faktory/wiki/Ent-Batches
// Except child batches, which are not implemented
// for a client to re-open a batch it must have a WID (worker id) applied to it
// that worker must be processing a job within the batch
type BatchSubsystem struct {
	Server  *server.Server
	Batches map[string]*batch
	mu      sync.Mutex
	Fetcher manager.Fetcher
	Options *Options
}

// NewBatchRequest structure for a new batch request
// Success and Complete are jobs to be queued
// once the batch has been committed and all jobs processed
type NewBatchRequest struct {
	//	ParentBid   string      `json:"parent_bid,omitempty"`
	Description string      `json:"description,omitempty"`
	Success     *client.Job `json:"success,omitempty"`
	Complete    *client.Job `json:"complete,omitempty"`
}

type Options struct {
	// Enabled - toggle for enabling the plugin
	Enabled bool
	// ChildSearchDepth - the n-th depth/level at which a batch will check if a child batch is done
	ChildSearchDepth int
	// UncommittedTimeout - number of minutes a batch is set to expire before committed
	UncommittedTimeout int
}

// Start - configures the batch subsystem
func (b *BatchSubsystem) Start(s *server.Server) error {
	b.Options = b.getOptions(s)
	if !b.Options.Enabled {
		return nil
	}
	b.Server = s
	b.mu = sync.Mutex{}
	b.Batches = make(map[string]*batch)
	b.Fetcher = manager.BasicFetcher(s.Manager().Redis())
	if err := b.loadExistingBatches(); err != nil {
		util.Warnf("loading existing batches: %v", err)
	}
	server.CommandSet["BATCH"] = b.batchCommand
	b.addMiddleware()
	util.Info("Loaded batching plugin")
	return nil
}

// Name - name of the plugin
func (b *BatchSubsystem) Name() string {
	return "Batch"
}

// Reload does not do anything
func (b *BatchSubsystem) Reload(s *server.Server) error {
	return nil
}

func (b *BatchSubsystem) getOptions(s *server.Server) *Options {
	enabledValue := s.Options.Config("batch", "enabled", false)
	enabled, ok := enabledValue.(bool)
	if !ok {
		enabled = false
	}
	childSearchDepthValue := s.Options.Config("batch", "child_search_depth", 0)
	childSearchDepth, ok := childSearchDepthValue.(int)
	if !ok {
		childSearchDepth = 0
	}

	uncommittedTimeoutValue := s.Options.Config("batch", "uncommitted_timeout", 120)
	uncommittedTimeout, ok := uncommittedTimeoutValue.(int)
	if !ok {
		uncommittedTimeout = 120
	}
	return &Options{
		Enabled: enabled,
		ChildSearchDepth: childSearchDepth,
		UncommittedTimeout: uncommittedTimeout,
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
		return nil, errors.New("getBatchFromInterface: invalid custom bid value")
	}
	batch, err := b.getBatch(bid)
	if err != nil {
		util.Warnf("getBatchFromInterface: Unable to retrieve batch: %v", err)
		return nil, fmt.Errorf("getBatchFromInterface: unable to get batch: %s", bid)
	}
	return batch, nil
}

func (b *BatchSubsystem) loadExistingBatches() error {
	vals, err := b.Server.Manager().Redis().SMembers("batches").Result()
	if err != nil {
		return fmt.Errorf("loadExistingBatches: retrieve batches: %v", err)
	}
	for idx := range vals {
		batch, err := b.newBatch(vals[idx], &batchMeta{})
		if err != nil {
			util.Warnf("loadExistingBatches: error load batch (%s) %v", vals[idx], err)
			continue
		}
		b.Batches[vals[idx]] = batch
	}

	// update parent and children
	for _, batch := range b.Batches {
		parentIds, err := b.Server.Manager().Redis().SMembers(batch.ParentsKey).Result()
		if err != nil {
			return fmt.Errorf("init: get parents: %v", err)
		}
		for _, parentId := range parentIds {
			parentBatch, err := b.getBatch(parentId)
			if err != nil {
				util.Warnf("loadExistingBatches: error getting parent batch (%s) %v", parentId, err)
				continue
			}
			batch.Parents = append(batch.Parents, parentBatch)
		}

		childIds, err := b.Server.Manager().Redis().SMembers(batch.ChildKey).Result()
		if err != nil {
			return fmt.Errorf("init: get parents: %v", err)
		}
		for _, childId := range childIds {
			childBatch, err := b.getBatch(childId)
			if err != nil {
				util.Warnf("loadExistingBatches: error getting child batch (%s) %v", childId, err)
				continue
			}
			batch.Children = append(batch.Children, childBatch)
		}

		batch.checkBatchDone()
	}

	return nil
}
func (b *BatchSubsystem) newBatchMeta(description string, success string, complete string) *batchMeta {
	return &batchMeta{
		CreatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		Total:            0,
		Succeeded:        0,
		Failed:           0,
		Pending:          0,
		Description:      description,
		SuccessJob:       success,
		CompleteJob:      complete,
		SuccessJobState:  CallbackJobPending,
		CompleteJobState: CallbackJobPending,
	}
}

func (b *BatchSubsystem) newBatch(batchId string, meta *batchMeta) (*batch, error) {
	batch := &batch{
		Id:         batchId,
		BatchKey:   fmt.Sprintf("batch-%s", batchId),
		JobsKey:    fmt.Sprintf("jobs-%s", batchId),
		MetaKey:    fmt.Sprintf("meta-%s", batchId),
		ParentsKey: fmt.Sprintf("parent-ids-%s", batchId),
		ChildKey:   fmt.Sprintf("child-ids-%s", batchId),
		Workers:    make(map[string]string),
		Jobs:       make([]string, 0),
		Parents:    make([]*batch, 0),
		Children:   make([]*batch, 0),
		Meta:       meta,
		rclient:    b.Server.Manager().Redis(),
		mu:         sync.Mutex{},
		Subsystem:  b,
	}
	if err := batch.init(); err != nil {
		return nil, fmt.Errorf("newBatch: %v", err)
	}

	b.Batches[batchId] = batch

	return batch, nil
}

func (b *BatchSubsystem) getBatch(batchId string) (*batch, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if batchId == "" {
		return nil, fmt.Errorf("getBatch: batchId cannot be blank")
	}

	batch, ok := b.Batches[batchId]

	if !ok {
		return nil, fmt.Errorf("getBatch: no batch found")
	}

	exists, err := b.Server.Manager().Redis().Exists(batch.BatchKey).Result()
	if err != nil {
		util.Warnf("Cannot confirm batch exists: %v", err)
		return nil, fmt.Errorf("getBatch: unable to check if batch has timed out")
	}
	if exists == 0 {
		b.removeBatch(batch)
		return nil, fmt.Errorf("getBatch: batch was not committed within 2 hours")
	}

	return batch, nil
}

func (b *BatchSubsystem) removeBatch(batch *batch) {
	if err := batch.remove(); err != nil {
		util.Warnf("removeBatch: unable to remove batch: %v", err)
	}
	delete(b.Batches, batch.Id)

	batch = nil
}
