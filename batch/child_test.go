package batch

import (
	"fmt"
	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestChildBatch(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	t.Run("Nested children", func(t *testing.T) {
		withServer(batchSystem, true, func(cl *client.Client) {
			var batchA *client.Batch
			var batchB *client.Batch
			var batchA1 *client.Batch
			var batchB1 *client.Batch
			var batchC1 *client.Batch
			var batchD1 *client.Batch

			b := client.NewBatch(cl)
			b.Description = "top build"
			b.Complete = client.NewJob("batchDone", 1, "string", 3)
			b.Success = client.NewJob("batchSuccess", 2, "string", 4)

			err := b.Jobs(func() error {
				err := b.Push(client.NewJob("A", 1))
				assert.Nil(t, err)
				batchA = client.NewBatch(cl)
				batchA.Description = "A"
				batchA.Complete = client.NewJob("A.batchDone", 1, "string", 3)
				batchA.Success = client.NewJob("A.batchSuccess", 2, "string", 4)
				_, err = cl.BatchNew(batchA)
				val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", b.Bid, batchA.Bid))
				assert.Nil(t, err)
				assert.Equal(t, val, "OK")

				err = b.Push(client.NewJob("B", 2))
				assert.Nil(t, err)
				batchB = client.NewBatch(cl)
				batchB.Description = "B"
				batchB.Complete = client.NewJob("B.batchDone", 1, "string", 3)
				batchB.Success = client.NewJob("B.batchSuccess", 2, "string", 4)
				_, err = cl.BatchNew(batchB)
				assert.Nil(t, err)
				val, err = cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", b.Bid, batchB.Bid))
				assert.Nil(t, err)
				assert.Equal(t, val, "OK")

				return nil
			})
			assert.Nil(t, err)
			batchData, err := batchSystem.batchManager.getBatch(b.Bid)
			assert.Nil(t, err)
			assert.Len(t, batchData.Children, 2)

			// job A depth 1
			err = processJob(cl, true, func(job *client.Job) {
				assert.Equal(t, "A", job.Type)
				batch, err := cl.BatchOpen(batchA.Bid)
				assert.Nil(t, err)
				err = batch.Jobs(func() error {
					err := batch.Push(client.NewJob("A.1", 1))
					assert.Nil(t, err)
					batchA1 = client.NewBatch(cl)
					batchA1.Description = "A1"
					batchA1.Complete = client.NewJob("A.1.batchDone", 1, "string", 3)
					batchA1.Success = client.NewJob("A.1.batchSuccess", 2, "string", 4)
					_, err = cl.BatchNew(batchA1)
					val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", batch.Bid, batchA1.Bid))
					assert.Nil(t, err)
					assert.Equal(t, val, "OK")

					val, err = cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", batchA1.Bid, batch.Bid))
					assert.Nil(t, err)
					assert.Equal(t, val, "OK")
					return nil
				})
				assert.Nil(t, err)
			})

			// job B depth 1
			err = processJob(cl, true, func(job *client.Job) {
				assert.Equal(t, "B", job.Type)
				batch, err := cl.BatchOpen(batchB.Bid)
				assert.Nil(t, err)
				err = batch.Jobs(func() error {
					err := batch.Push(client.NewJob("B.1", 1))
					assert.Nil(t, err)
					batchB1 = client.NewBatch(cl)
					batchB1.Description = "B1"
					batchB1.Complete = client.NewJob("B.1.batchDone", 1, "string", 3)
					batchB1.Success = client.NewJob("B.1.batchSuccess", 2, "string", 4)
					_, err = cl.BatchNew(batchB1)
					val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", batch.Bid, batchB1.Bid))
					assert.Nil(t, err)
					assert.Equal(t, val, "OK")

					return nil
				})
				assert.Nil(t, err)
			})

			// job A.1 depth 2
			err = processJob(cl, true, func(job *client.Job) {
				assert.Equal(t, "A.1", job.Type)
				batch, err := cl.BatchOpen(batchA1.Bid)
				assert.Nil(t, err)
				err = batch.Jobs(func() error {
					err := batch.Push(client.NewJob("C.1", 1))
					assert.Nil(t, err)
					batchC1 = client.NewBatch(cl)
					batchC1.Description = "C1"
					batchC1.Complete = client.NewJob("C.1.batchDone", 1, "string", 3)
					batchC1.Success = client.NewJob("C.1.batchSuccess", 2, "string", 4)
					_, err = cl.BatchNew(batchC1)
					val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", batch.Bid, batchC1.Bid))
					assert.Nil(t, err)
					assert.Equal(t, val, "OK")

					return nil
				})
				assert.Nil(t, err)
			})

			// job B.1 depth 2
			err = processJob(cl, true, func(job *client.Job) {
				assert.Equal(t, "B.1", job.Type)
				batch, err := cl.BatchOpen(batchB1.Bid)
				assert.Nil(t, err)
				err = batch.Jobs(func() error {
					err := batch.Push(client.NewJob("D.1", 1))
					assert.Nil(t, err)
					batchD1 = client.NewBatch(cl)
					batchD1.Description = "D1"
					batchD1.Complete = client.NewJob("D.1.batchDone", 1, "string", 3)
					batchD1.Success = client.NewJob("D.1.batchSuccess", 2, "string", 4)
					_, err = cl.BatchNew(batchD1)
					val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", batch.Bid, batchD1.Bid))
					assert.Nil(t, err)
					assert.Equal(t, val, "OK")

					return nil
				})
				assert.Nil(t, err)
			})

			// job C.1 depth 3 relies on D
			err = processJob(cl, true, func(job *client.Job) {
				assert.Equal(t, "C.1", job.Type)
				batch, err := cl.BatchOpen(batchC1.Bid)
				assert.Nil(t, err)
				val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", batch.Bid, batchD1.Bid))
				assert.Nil(t, err)
				assert.Equal(t, val, "OK")
				err = batch.Commit()
				assert.Nil(t, err)
			})

			// job D.1 depth 3 from B.1 / depth 4 from C.1
			err = processJob(cl, true, func(job *client.Job) {
				assert.Equal(t, "D.1", job.Type)
				batch, err := cl.BatchOpen(batchD1.Bid)
				assert.Nil(t, err)
				// circular reference
				val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", batch.Bid, batchA1.Bid))
				assert.Equal(t, val, "OK")
				assert.Nil(t, err)
				err = batch.Commit()
				assert.Nil(t, err)
			})

			assert.Nil(t, err)
			assert.Equal(t, 2, batchData.Meta.Succeeded)
			assert.Equal(t, 0, batchData.Meta.Failed)
			assert.Equal(t, 0, batchData.Meta.Pending)
			assert.True(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))

			// callback jobs
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "D.1.batchDone", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "D.1.batchSuccess", job.Type)
			})
			assert.Nil(t, err)

			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "C.1.batchDone", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "C.1.batchSuccess", job.Type)
			})
			assert.Nil(t, err)

			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "A.1.batchDone", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "A.1.batchSuccess", job.Type)
			})
			assert.Nil(t, err)

			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "A.batchDone", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "A.batchSuccess", job.Type)
			})
			assert.Nil(t, err)

			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "B.1.batchDone", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "B.1.batchSuccess", job.Type)
			})
			assert.Nil(t, err)

			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "B.batchDone", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "B.batchSuccess", job.Type)
			})
			assert.Nil(t, err)

			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "batchDone", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "batchSuccess", job.Type)
			})
			assert.Nil(t, err)

			// batch A1 and B1 have not been committed as this point
			assert.Equal(t, uint64(0), batchSystem.Server.Store().Scheduled().Size())
			def, _ := batchSystem.Server.Store().GetQueue("default")
			assert.Equal(t, uint64(0), def.Size())
		})
	})

	t.Run("Set child depth", func(t *testing.T) {
		withServer(batchSystem, true, func(cl *client.Client) {
			var batchA *client.Batch
			var batchB *client.Batch
			var batchA1 *client.Batch

			b := client.NewBatch(cl)
			b.Description = "top build"
			b.Complete = client.NewJob("batchDone", 1, "string", 3)
			b.Success = client.NewJob("batchSuccess", 2, "string", 4)

			err := b.Jobs(func() error {
				err := b.Push(client.NewJob("A", 1))
				assert.Nil(t, err)
				batchA = client.NewBatch(cl)
				batchA.Description = "A"
				batchA.Complete = client.NewJob("A.batchDone", 1, "string", 3)
				batchA.Success = client.NewJob("A.batchSuccess", 2, "string", 4)
				_, err = cl.BatchNew(batchA)
				val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", b.Bid, batchA.Bid))
				assert.Nil(t, err)
				assert.Equal(t, val, "OK")

				err = b.Push(client.NewJob("B", 2))
				assert.Nil(t, err)
				batchB = client.NewBatch(cl)
				batchB.Description = "B"
				batchB.Complete = client.NewJob("B.batchDone", 1, "string", 3)
				batchB.Success = client.NewJob("B.batchSuccess", 2, "string", 4)
				_, err = cl.BatchNew(batchB)
				assert.Nil(t, err)
				val, err = cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", b.Bid, batchB.Bid))
				assert.Nil(t, err)
				assert.Equal(t, val, "OK")

				return nil
			})
			assert.Nil(t, err)
			batchData, err := batchSystem.batchManager.getBatch(b.Bid)
			assert.Nil(t, err)
			assert.Len(t, batchData.Children, 2)
			depth := 1
			batchData.Meta.ChildSearchDepth = &depth

			// job A depth 1
			err = processJob(cl, true, func(job *client.Job) {
				assert.Equal(t, "A", job.Type)
				batch, err := cl.BatchOpen(batchA.Bid)
				assert.Nil(t, err)
				err = batch.Jobs(func() error {
					err := batch.Push(client.NewJob("A.1", 1))
					assert.Nil(t, err)
					batchA1 = client.NewBatch(cl)
					batchA1.Description = "A1"
					batchA1.Complete = client.NewJob("A.1.batchDone", 1, "string", 3)
					batchA1.Success = client.NewJob("A.1.batchSuccess", 2, "string", 4)
					_, err = cl.BatchNew(batchA1)
					val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", batch.Bid, batchA1.Bid))
					assert.Nil(t, err)
					assert.Equal(t, val, "OK")

					return nil
				})
				assert.Nil(t, err)
			})

			// job B depth 1
			err = processJob(cl, true, func(job *client.Job) {
				assert.Equal(t, "B", job.Type)
				batch, err := cl.BatchOpen(batchB.Bid)
				assert.Nil(t, err)
				err = batch.Jobs(func() error {
					err := batch.Push(client.NewJob("B.1", 1))
					assert.Nil(t, err)

					return nil
				})
				assert.Nil(t, err)
			})

			assert.Nil(t, err)
			assert.Equal(t, 2, batchData.Meta.Succeeded)
			assert.Equal(t, 0, batchData.Meta.Failed)
			assert.Equal(t, 0, batchData.Meta.Pending)
			assert.True(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))

			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "A.1", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "B.1", job.Type)
			})
			assert.Nil(t, err)

			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "B.batchDone", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "B.batchSuccess", job.Type)
			})
			assert.Nil(t, err)

			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "batchDone", job.Type)
			})
			assert.Nil(t, err)
			err = processJob(cl, true, func(job *client.Job) {
				assert.NotNil(t, job)
				assert.Equal(t, "batchSuccess", job.Type)
			})
			assert.Nil(t, err)
			assert.Equal(t, uint64(0), batchSystem.Server.Store().Scheduled().Size())
			def, _ := batchSystem.Server.Store().GetQueue("default")
			assert.Equal(t, uint64(0), def.Size())
		})
	})

	t.Run("Batch multiple parents", func(t *testing.T) {
		withServer(batchSystem, true, func(cl *client.Client) {
			var batchA *client.Batch
			b := client.NewBatch(cl)
			b.Description = "top build"
			b.Complete = client.NewJob("batchDone", 1, "string", 3)
			b.Success = client.NewJob("batchSuccess", 2, "string", 4)
			err := b.Jobs(func() error {
				err := b.Push(client.NewJob("A", 1))
				assert.Nil(t, err)
				batchA = client.NewBatch(cl)
				batchA.Complete = client.NewJob("batchDone", 1, "string", 3)
				batchA.Success = client.NewJob("batchSuccess", 2, "string", 4)
				_, err = cl.BatchNew(batchA)
				val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", b.Bid, batchA.Bid))
				assert.Nil(t, err)
				assert.Equal(t, val, "OK")
				return nil
			})
			assert.Nil(t, err)

			// top leve build
			b2 := client.NewBatch(cl)
			b2.Description = "top build 2"
			b2.Complete = client.NewJob("batchDone", 1, "string", 3)
			b2.Success = client.NewJob("batchSuccess", 2, "string", 4)

			err = b2.Jobs(func() error {
				val, err := cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", b2.Bid, batchA.Bid))

				err = b2.Push(client.NewJob("B", 2))
				assert.Nil(t, err)
				batchB := client.NewBatch(cl)
				batchB.Description = "B"
				batchB.Complete = client.NewJob("B.batchDone", 1, "string", 3)
				batchB.Success = client.NewJob("B.batchSuccess", 2, "string", 4)
				_, err = cl.BatchNew(batchB)
				assert.Nil(t, err)
				val, err = cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", b2.Bid, batchB.Bid))
				assert.Nil(t, err)
				assert.Equal(t, val, "OK")

				return nil
			})
			assert.Nil(t, err)
			topBatch, err := batchSystem.batchManager.getBatch(b.Bid)
			assert.Nil(t, err)
			assert.Len(t, topBatch.Children, 1)

			topBatch2, err := batchSystem.batchManager.getBatch(b2.Bid)
			assert.Nil(t, err)
			assert.Len(t, topBatch2.Children, 2)
			// build A
			cl2, err := getClient()
			defer cl2.Close()
			assert.Nil(t, err)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				err = processJob(cl, true, nil)
				assert.Nil(t, err)
			}()

			// build B
			go func() {
				defer wg.Done()
				err = processJob(cl2, true, nil)
				assert.Nil(t, err)
			}()

			wg.Wait()
			assert.True(t, batchSystem.batchManager.areBatchJobsCompleted(topBatch))
			assert.True(t, batchSystem.batchManager.areBatchJobsCompleted(topBatch2))
		})
	})
}
