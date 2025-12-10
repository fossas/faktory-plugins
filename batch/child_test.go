package batch

import (
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

			// Wait a moment
			time.Sleep(200 * time.Millisecond)

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

			// Wait for child callback
			time.Sleep(200 * time.Millisecond)

			// Child callback should fire
			childCallback, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, childCallback, "child callback should fire")
			assert.Equal(t, "ChildCallback", childCallback.Type)

			// ACK child callback
			err = cl.Ack(childCallback.Jid)
			require.NoError(t, err)

			// Wait for parent callback
			time.Sleep(200 * time.Millisecond)

			// Now parent callback should fire
			parentCallback, err = cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, parentCallback, "parent callback should fire after child callback")
			assert.Equal(t, "ParentCallback", parentCallback.Type)
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

			// Wait for child callback (should fire first since empty)
			time.Sleep(200 * time.Millisecond)

			// Child callback should fire first
			callback1, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callback1, "child callback should fire")
			assert.Equal(t, "ChildCallback", callback1.Type)

			// ACK child callback
			err = cl.Ack(callback1.Jid)
			require.NoError(t, err)

			// Wait for parent callback
			time.Sleep(200 * time.Millisecond)

			// Parent callback should fire next
			callback2, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callback2, "parent callback should fire")
			assert.Equal(t, "ParentCallback", callback2.Type)

			// ACK parent callback
			err = cl.Ack(callback2.Jid)
			require.NoError(t, err)

			// Wait for grandparent callback
			time.Sleep(200 * time.Millisecond)

			// Grandparent callback should fire last
			callback3, err := cl.Fetch("callbacks")
			require.NoError(t, err)
			require.NotNil(t, callback3, "grandparent callback should fire")
			assert.Equal(t, "GrandparentCallback", callback3.Type)
		})
	})
}
