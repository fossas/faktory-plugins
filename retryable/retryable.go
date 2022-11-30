package retryable

import (
	"encoding/json"
	"fmt"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

var _ server.Subsystem = &RetryableSubsystem{}

// RetryableSubsystem allows a client to indicate that it is not able to complete
// the job and that it should be retried.
type RetryableSubsystem struct{}

// Start loads the plugin.
func (r *RetryableSubsystem) Start(s *server.Server) error {
	server.CommandSet["RETRY"] = r.retryCommand
	util.Info("Loaded the retryable jobs plugin")
	return nil
}

// Name returns the name of the plugin.
func (r *RetryableSubsystem) Name() string {
	return "RetryableJobs"
}

// Reload does not do anything.
func (r *RetryableSubsystem) Reload(s *server.Server) error {
	return nil
}

// retryCommand implements the RETRY client command which simply ACKs and
// requeues the job.
// RETRY {"jid":"123456789"}
func (r *RetryableSubsystem) retryCommand(c *server.Connection, s *server.Server, cmd string) {
	data := cmd[6:]

	var hash map[string]string
	err := json.Unmarshal([]byte(data), &hash)
	if err != nil {
		_ = c.Error(cmd, fmt.Errorf("invalid RETRY %s", data))
		return
	}
	jid, ok := hash["jid"]
	if !ok {
		_ = c.Error(cmd, fmt.Errorf("invalid RETRY %s", data))
		return
	}
	job, err := s.Manager().Acknowledge(jid)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}

	jdata, err := json.Marshal(job)
	if err != nil {
		_ = c.Error(cmd, err)
		return
	}
	s.Manager().Redis().RPush(job.Queue, jdata)

	_ = c.Ok()
}
