package batch

import (
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"sync"
)

// BatchSubsystem enables jobs to be grouped into a batch
// the implementation follows the spec here: https://github.com/contribsys/faktory/wiki/Ent-Batches
// Except child batches, which are not implemented
type BatchSubsystem struct {
	Server       *server.Server
	batchManager *batchManager
	Fetcher      manager.Fetcher
	Options      *Options
}

type Options struct {
	// Enabled - toggle for enabling the plugin
	Enabled bool
	// ChildSearchDepth - the n-th depth/level at which a batch will check if a child batch is done
	ChildSearchDepth int
	// UncommittedTimeout - number of minutes a batch is set to expire before committed
	UncommittedTimeoutMinutes int
	// CommittedTimeout - number of days long a can exist after its been commited
	CommittedTimeoutDays int
}

// Start - configures the batch subsystem
func (b *BatchSubsystem) Start(s *server.Server) error {
	b.Options = b.getOptions(s)
	if !b.Options.Enabled {
		return nil
	}
	b.Server = s
	b.batchManager = &batchManager{
		Batches:   make(map[string]*batch),
		mu:        sync.Mutex{},
		rclient:   b.Server.Manager().Redis(),
		Subsystem: b,
	}
	b.Fetcher = manager.BasicFetcher(s.Manager().Redis())
	if err := b.batchManager.loadExistingBatches(); err != nil {
		util.Warnf("loading existing batches: %v", err)
	}
	server.CommandSet["BATCH"] = b.batchCommand
	b.addMiddleware()

	b.Server.AddTask(3600*24, &removeStaleBatches{b}) // once a day
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
	childSearchDepth, ok := childSearchDepthValue.(int64)
	if !ok {
		childSearchDepth = 0
	}

	uncommittedTimeoutValue := s.Options.Config("batch", "uncommitted_timeout_minutes", 120)
	uncommittedTimeout, ok := uncommittedTimeoutValue.(int64)
	if !ok {
		uncommittedTimeout = 120
	}

	committedTimeoutValue := s.Options.Config("batch", "committed_timeout_days", 7)
	committedTimeout, ok := committedTimeoutValue.(int64)
	if !ok {
		committedTimeout = 7
	}

	return &Options{
		Enabled:                   enabled,
		ChildSearchDepth:          int(childSearchDepth),
		UncommittedTimeoutMinutes: int(uncommittedTimeout),
		CommittedTimeoutDays:      int(committedTimeout),
	}
}

func (b *BatchSubsystem) addMiddleware() {
	b.Server.Manager().AddMiddleware("push", b.pushMiddleware)
	b.Server.Manager().AddMiddleware("ack", b.handleJobFinished(true))
	b.Server.Manager().AddMiddleware("fail", b.handleJobFinished(false))
}
