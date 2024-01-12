package batch

import (
	"context"

	"github.com/contribsys/faktory/server"
)

var _ server.Taskable = &removeStaleBatches{}

type removeStaleBatches struct {
	Subsystem *BatchSubsystem
}

// Name - name of the task
func (t *removeStaleBatches) Name() string {
	return "RemoveStaleBatches"
}

// Execute - runs the task to collect metrics
func (t *removeStaleBatches) Execute(ctx context.Context) error {
	go t.Subsystem.batchManager.removeStaleBatches(ctx)
	return nil
}

// Stats - this is required but there are no useful stats to record for batches
// so return an empty map
func (t *removeStaleBatches) Stats(ctx context.Context) map[string]interface{} {
	// no stats
	return map[string]interface{}{}
}
