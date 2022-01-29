package batch

import (
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
func (t *removeStaleBatches) Execute() error {
	t.Subsystem.batchManager.removeStaleBatches()
	return nil
}

// Stats - this is required but there are no useful stats to record for batches
// so return an empty map
func (t *removeStaleBatches) Stats() map[string]interface{} {
	// no stats
	return map[string]interface{}{}
}
