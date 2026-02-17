package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
)

// batchCommand is the main entry point for all BATCH subcommands
func (b *BatchSubsystem) batchCommand(c *server.Connection, s *server.Server, cmd string) {
	// Parse subcommand: BATCH NEW|COMMIT|OPEN|STATUS <args>
	parts := strings.SplitN(cmd, " ", 3)
	if len(parts) < 2 {
		_ = c.Error(cmd, fmt.Errorf("invalid BATCH command: missing subcommand"))
		return
	}

	subcommand := strings.ToUpper(parts[1])
	var args string
	if len(parts) > 2 {
		args = parts[2]
	}

	switch subcommand {
	case "NEW":
		b.batchNew(c, s, args)
	case "COMMIT":
		b.batchCommit(c, s, args)
	case "OPEN":
		b.batchOpen(c, s, args)
	case "STATUS":
		b.batchStatus(c, s, args)
	default:
		_ = c.Error(cmd, fmt.Errorf("unknown BATCH subcommand: %s", subcommand))
	}
}

// batchNew handles BATCH NEW {json}
// Creates a new batch and returns the generated BID
func (b *BatchSubsystem) batchNew(c *server.Connection, s *server.Server, data string) {
	if data == "" {
		_ = c.Error("BATCH NEW", fmt.Errorf("missing batch definition"))
		return
	}

	var batch client.Batch
	if err := json.Unmarshal([]byte(data), &batch); err != nil {
		_ = c.Error("BATCH NEW", fmt.Errorf("invalid JSON: %w", err))
		return
	}

	// BID must not be specified by client
	if batch.Bid != "" {
		_ = c.Error("BATCH NEW", fmt.Errorf("BID must be blank when creating a new Batch, cannot specify it"))
		return
	}

	// Must have at least one callback
	if batch.Success == nil && batch.Complete == nil {
		_ = c.Error("BATCH NEW", fmt.Errorf("batch must have at least one callback (success or complete)"))
		return
	}

	// Generate BID
	batch.Bid = generateBid()

	// Create batch in Redis
	ctx := c.Context
	if err := createBatch(ctx, s, &batch); err != nil {
		_ = c.Error("BATCH NEW", err)
		return
	}

	// If parent_bid specified, add as child of parent
	if batch.ParentBid != "" {
		if err := addChildBatch(ctx, s, batch.ParentBid, batch.Bid); err != nil {
			// Clean up the created batch on failure
			_ = deleteBatch(ctx, s, batch.Bid)
			_ = c.Error("BATCH NEW", fmt.Errorf("failed to add child batch: %w", err))
			return
		}
	}

	util.Debugf("Created batch %s", batch.Bid)

	// Return BID as bulk string
	_ = c.Result([]byte(batch.Bid))
}

// batchCommit handles BATCH COMMIT <bid>
// Marks the batch as committed, enabling callbacks to fire
func (b *BatchSubsystem) batchCommit(c *server.Connection, s *server.Server, bid string) {
	bid = strings.TrimSpace(bid)
	if bid == "" {
		_ = c.Error("BATCH COMMIT", fmt.Errorf("missing batch ID"))
		return
	}

	ctx := c.Context

	// Check batch exists
	exists, err := batchExists(ctx, s, bid)
	if err != nil {
		_ = c.Error("BATCH COMMIT", err)
		return
	}
	if !exists {
		_ = c.Error("BATCH COMMIT", fmt.Errorf("batch %s not found", bid))
		return
	}

	// Mark as committed
	if err := setCommitted(ctx, s, bid); err != nil {
		_ = c.Error("BATCH COMMIT", err)
		return
	}

	util.Debugf("Committed batch %s", bid)

	// Check if callbacks should fire (e.g., empty batch)
	go b.checkAndFireCallbacks(context.Background(), s, bid)

	_ = c.Ok()
}

// batchOpen handles BATCH OPEN <bid>
// Reopens a committed batch to allow adding more jobs
// Note: We do not check if the client calling `BATCH OPEN` is working on a job in the batch. The architecture of the
// workers in our Core application does not allow us to do this.
func (b *BatchSubsystem) batchOpen(c *server.Connection, s *server.Server, bid string) {
	bid = strings.TrimSpace(bid)
	if bid == "" {
		_ = c.Error("BATCH OPEN", fmt.Errorf("missing batch ID"))
		return
	}

	ctx := c.Context

	// Check batch exists
	exists, err := batchExists(ctx, s, bid)
	if err != nil {
		_ = c.Error("BATCH OPEN", err)
		return
	}
	if !exists {
		_ = c.Error("BATCH OPEN", fmt.Errorf("batch %s not found", bid))
		return
	}

	// Atomically check callbacks haven't started and uncommit
	if err := setUncommitted(ctx, s, bid); err != nil {
		_ = c.Error("BATCH OPEN", err)
		return
	}

	util.Debugf("Opened batch %s", bid)

	// Return BID as confirmation
	_ = c.Result([]byte(bid))
}

// batchStatus handles BATCH STATUS <bid>
// Returns the current status of a batch as JSON
func (b *BatchSubsystem) batchStatus(c *server.Connection, s *server.Server, bid string) {
	bid = strings.TrimSpace(bid)
	if bid == "" {
		_ = c.Error("BATCH STATUS", fmt.Errorf("missing batch ID"))
		return
	}

	ctx := c.Context

	status, err := getBatchStatus(ctx, s, bid)
	if err != nil {
		_ = c.Error("BATCH STATUS", err)
		return
	}

	data, err := json.Marshal(status)
	if err != nil {
		_ = c.Error("BATCH STATUS", fmt.Errorf("failed to marshal status: %w", err))
		return
	}

	_ = c.Result(data)
}
