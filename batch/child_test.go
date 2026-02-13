package batch

import (
	"fmt"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChildBatch(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("creates child batch", func(t *testing.T) {
			// Create parent batch
			parentResult, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"ParentCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			parentBid := string(parentResult)

			// Create child batch with parent_bid
			childResult, err := cl.Generic(`BATCH NEW {"parent_bid":"` + parentBid + `","success":{"jobtype":"ChildCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			childBid := string(childResult)

			assert.NotEmpty(t, childBid)
			assert.NotEqual(t, parentBid, childBid)
		})

		t.Run("rejects child batch with non-existent parent", func(t *testing.T) {
			_, err := cl.Generic(`BATCH NEW {"parent_bid":"non-existent-parent","success":{"jobtype":"Callback"}}`)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})
	})
}

func TestChildBlocksParentCallback(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("parent waits for child callback", func(t *testing.T) {
			// Create parent batch
			parentResult, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"ParentCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			parentBid := string(parentResult)

			// Push a job to parent
			parentJob := client.NewJob("ParentJob", 1)
			parentJob.SetCustom("bid", parentBid)
			err = cl.Push(parentJob)
			require.NoError(t, err)

			// Create child batch
			childResult, err := cl.Generic(`BATCH NEW {"parent_bid":"` + parentBid + `","success":{"jobtype":"ChildCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			childBid := string(childResult)

			// Push a job to child
			childJob := client.NewJob("ChildJob", 1)
			childJob.SetCustom("bid", childBid)
			err = cl.Push(childJob)
			require.NoError(t, err)

			// Commit both batches
			_, err = cl.Generic("BATCH COMMIT " + childBid)
			require.NoError(t, err)
			_, err = cl.Generic("BATCH COMMIT " + parentBid)
			require.NoError(t, err)

			// Complete parent job
			fetchedParentJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedParentJob)
			err = cl.Ack(fetchedParentJob.Jid)
			require.NoError(t, err)

			// Parent callback should not fire yet (child not done)
			parentCallback, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			assert.Nil(t, parentCallback, "parent callback should not fire until child completes")

			// Complete child job
			fetchedChildJob, err := cl.Fetch("default")
			require.NoError(t, err)
			require.NotNil(t, fetchedChildJob)
			err = cl.Ack(fetchedChildJob.Jid)
			require.NoError(t, err)

			// Child callback should fire
			childCallback, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, childCallback, "child callback should fire")
			assert.Equal(t, "ChildCallback", childCallback.Type)

			// ACK child callback
			err = cl.Ack(childCallback.Jid)
			require.NoError(t, err)

			// Now parent callback should fire
			parentCallback, err = cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, parentCallback, "parent callback should fire after child callback")
			assert.Equal(t, "ParentCallback", parentCallback.Type)
		})
	})
}

func TestChildFailureBlocksParentSuccess(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("parent success callback does not fire when child has failures", func(t *testing.T) {
			// BUG 3: Child batch with failures should prevent parent success callback.
			// Previously, the child would be deleted (because failed>0 meant success
			// would never fire), and the parent couldn't see the child's failure state.

			// Create parent batch with success callback
			parentResult, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"ParentSuccess","queue":"callbacks"},"complete":{"jobtype":"ParentComplete","queue":"callbacks"}}`)
			require.NoError(t, err)
			parentBid := string(parentResult)

			// Create child batch with success and complete callbacks
			childResult, err := cl.Generic(`BATCH NEW {"parent_bid":"` + parentBid + `","success":{"jobtype":"ChildSuccess","queue":"callbacks"},"complete":{"jobtype":"ChildComplete","queue":"callbacks"}}`)
			require.NoError(t, err)
			childBid := string(childResult)

			// Push jobs to separate queues so we can fetch them independently
			childJob := client.NewJob("ChildJob", 1)
			childJob.Queue = "child_jobs"
			childJob.SetCustom("bid", childBid)
			retry := 0
			childJob.Retry = &retry
			err = cl.Push(childJob)
			require.NoError(t, err)

			parentJob := client.NewJob("ParentJob", 1)
			parentJob.Queue = "parent_jobs"
			parentJob.SetCustom("bid", parentBid)
			err = cl.Push(parentJob)
			require.NoError(t, err)

			// Commit both batches
			_, err = cl.Generic("BATCH COMMIT " + childBid)
			require.NoError(t, err)
			_, err = cl.Generic("BATCH COMMIT " + parentBid)
			require.NoError(t, err)

			// Complete parent job
			fetchedParentJob, err := cl.Fetch("parent_jobs")
			require.NoError(t, err)
			require.NotNil(t, fetchedParentJob)
			err = cl.Ack(fetchedParentJob.Jid)
			require.NoError(t, err)

			// Fail child job terminally
			fetchedChildJob, err := cl.Fetch("child_jobs")
			require.NoError(t, err)
			require.NotNil(t, fetchedChildJob)
			err = cl.Fail(fetchedChildJob.Jid, fmt.Errorf("terminal failure"), nil)
			require.NoError(t, err)

			// Wait for processing
			time.Sleep(300 * time.Millisecond)

			// Child complete callback should fire (pending=0 after first execution)
			childComplete, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, childComplete, "child complete callback should fire")
			assert.Equal(t, "ChildComplete", childComplete.Type)

			// ACK child complete callback
			err = cl.Ack(childComplete.Jid)
			require.NoError(t, err)

			// Wait for processing
			time.Sleep(300 * time.Millisecond)

			// Parent complete callback should fire (all children's complete callbacks finished)
			parentComplete, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, parentComplete, "parent complete callback should fire")
			assert.Equal(t, "ParentComplete", parentComplete.Type)

			// ACK parent complete callback
			err = cl.Ack(parentComplete.Jid)
			require.NoError(t, err)

			// Wait for processing
			time.Sleep(300 * time.Millisecond)

			// Child had failures, so child success callback should NOT fire.
			// Parent success callback should also NOT fire because child had failures.
			remaining, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			assert.Nil(t, remaining, "parent success callback should not fire when child had failures")
		})
	})
}

func TestNestedBatches(t *testing.T) {
	withServer(func(s *server.Server, cl *client.Client) {
		t.Run("three level nesting works", func(t *testing.T) {
			// Create grandparent batch
			grandparentResult, err := cl.Generic(`BATCH NEW {"success":{"jobtype":"GrandparentCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			grandparentBid := string(grandparentResult)

			// Create parent batch
			parentResult, err := cl.Generic(`BATCH NEW {"parent_bid":"` + grandparentBid + `","success":{"jobtype":"ParentCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			parentBid := string(parentResult)

			// Create child batch
			childResult, err := cl.Generic(`BATCH NEW {"parent_bid":"` + parentBid + `","success":{"jobtype":"ChildCallback","queue":"callbacks"}}`)
			require.NoError(t, err)
			childBid := string(childResult)

			// Commit all batches (empty batches - callbacks fire when child callbacks complete)
			_, err = cl.Generic("BATCH COMMIT " + childBid)
			require.NoError(t, err)
			_, err = cl.Generic("BATCH COMMIT " + parentBid)
			require.NoError(t, err)
			_, err = cl.Generic("BATCH COMMIT " + grandparentBid)
			require.NoError(t, err)

			// Child callback should fire first
			callback1, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callback1, "child callback should fire")
			assert.Equal(t, "ChildCallback", callback1.Type)

			// ACK child callback
			err = cl.Ack(callback1.Jid)
			require.NoError(t, err)

			// Parent callback should fire next
			callback2, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callback2, "parent callback should fire")
			assert.Equal(t, "ParentCallback", callback2.Type)

			// ACK parent callback
			err = cl.Ack(callback2.Jid)
			require.NoError(t, err)

			// Grandparent callback should fire last
			callback3, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callback3, "grandparent callback should fire")
			assert.Equal(t, "GrandparentCallback", callback3.Type)
		})
	})
}
