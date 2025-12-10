package batch

import (
	"context"
)

// cleanupTask is a background task that can be used for batch maintenance
// Note: Batch cleanup happens automatically when callbacks complete (in callbacks.go)
// This task is primarily for monitoring and stats
type cleanupTask struct {
	subsystem *BatchSubsystem
	cycles    int64
}

// Name returns the name of the task
func (t *cleanupTask) Name() string {
	return "BatchMaintenance"
}

// Execute runs the maintenance task
func (t *cleanupTask) Execute(ctx context.Context) error {
	t.cycles++
	// Batch cleanup happens automatically when callbacks complete
	// This task is reserved for future maintenance needs
	return nil
}

// Stats returns statistics about the task
func (t *cleanupTask) Stats(ctx context.Context) map[string]interface{} {
	return map[string]interface{}{
		"cycles": t.cycles,
	}
}
