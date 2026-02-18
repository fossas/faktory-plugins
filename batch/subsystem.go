package batch

import (
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/faktory/webui"
)

// Ensure BatchSubsystem implements server.Subsystem
var _ server.Subsystem = &BatchSubsystem{}

// BatchSubsystem implements the Faktory Job Batching feature.
// It allows grouping jobs into batches with success/complete callbacks.
type BatchSubsystem struct {
	Server *server.Server
}

// Start initializes the batch subsystem
func (b *BatchSubsystem) Start(s *server.Server) error {
	b.Server = s

	// Register BATCH command
	server.CommandSet["BATCH"] = b.batchCommand

	// Register middleware for job tracking
	b.addMiddleware()

	// Register cleanup task
	s.AddTask(60, &batchSweepTask{subsystem: b})

	// Find WebUI lifecycle and register routes + tab
	for _, sub := range s.Subsystems {
		if lifecycle, ok := sub.(*webui.Lifecycle); ok {
			ui := lifecycle.WebUI
			webui.DefaultTabs = append(webui.DefaultTabs, webui.Tab{
				Name: "Batches",
				Path: "/batches",
			})
			ui.App.HandleFunc("/batches", webui.Log(ui, b.batchesHandler))
			ui.App.HandleFunc("/batches/", webui.Log(ui, b.batchDetailHandler))
			break
		}
	}

	util.Info("Loaded batch jobs plugin")
	return nil
}

// Name returns the name of the subsystem
func (b *BatchSubsystem) Name() string {
	return "Batch"
}

// Reload reloads the subsystem configuration
func (b *BatchSubsystem) Reload(s *server.Server) error {
	b.Server = s
	return nil
}

// Shutdown gracefully shuts down the subsystem
func (b *BatchSubsystem) Shutdown(s *server.Server) error {
	return nil
}

// addMiddleware registers the batch middleware for push, ack, and fail operations
func (b *BatchSubsystem) addMiddleware() {
	b.Server.Manager().AddMiddleware("push", b.pushMiddleware)
	b.Server.Manager().AddMiddleware("ack", b.ackMiddleware)
	b.Server.Manager().AddMiddleware("fail", b.failMiddleware)
}
