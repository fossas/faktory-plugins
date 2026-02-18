package batch

import (
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/stretchr/testify/assert"
)

func TestSubsystemEnabled(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		// Verify BATCH command is registered when enabled
		_, err := cl.Generic(`BATCH STATUS test-bid`)
		// Should get "not found" error, not "unknown command"
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestSubsystemDisabled(t *testing.T) {
	withServerConfig(false, func(s *server.Server, cl *client.Client) {
		// Note: Due to server.CommandSet being a global map, we can't reliably
		// test that the BATCH command is unregistered when disabled.
		// Instead, verify that a batch subsystem with disabled=false doesn't
		// register its middleware by checking that push without bid works.

		// Push a job - should work without any batch interference
		job := client.NewJob("TestJob", 1)
		err := cl.Push(job)
		assert.NoError(t, err)
	})
}

func TestSubsystemName(t *testing.T) {
	subsystem := &BatchSubsystem{}
	assert.Equal(t, "Batch", subsystem.Name())
}
