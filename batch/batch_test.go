package batch

import (
	"errors"
	"fmt"
	"github.com/contribsys/faktory/util"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
)

func TestBatchSuccess(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, true, func(cl *client.Client) {
		b := client.NewBatch(cl)

		b.Complete = client.NewJob("batchDone", 1, "string", 3)
		b.Success = client.NewJob("batchSuccess", 2, "string", 4)
		b.Description = "Test batch"

		err := b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)
			err = b.Push(client.NewJob("JobTwo", 2))
			assert.Nil(t, err)
			return nil
		})
		assert.Nil(t, err)
		assert.NotEqual(t, "", b.Bid)

		time.Sleep(1 * time.Second)
		batchData, err := batchSystem.batchManager.getBatch(b.Bid)
		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Total)

		// job one
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, 0, batchData.Meta.Succeeded)
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
		assert.False(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))

		// job two
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, batchData.Meta.Succeeded, 1)
			assert.Equal(t, batchData.Meta.Failed, 0)
		})

		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Succeeded)
		assert.Equal(t, 0, batchData.Meta.Failed)
		assert.True(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))

		assert.Equal(t, "1", batchData.Meta.CompleteJobState)
		assert.Equal(t, "1", batchData.Meta.SuccessJobState)
		// completeJob
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, "batchDone", job.Type)
		})
		assert.Nil(t, err)
		assert.Equal(t, "2", batchData.Meta.CompleteJobState)
		// successJob
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, "batchSuccess", job.Type)
		})
		assert.Nil(t, err)
		assert.Equal(t, "2", batchData.Meta.SuccessJobState)

		fetchedJob, err := cl.Fetch("default")
		assert.Nil(t, fetchedJob)

		_, err = batchSystem.batchManager.getBatch(b.Bid)

		// ensure batch is removed
		assert.Error(t, err)
		assert.EqualError(t, err, "getBatch: no batch found")
	})
}

func TestBatchSuccessWithoutSuccessCallback(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, true, func(cl *client.Client) {
		b := client.NewBatch(cl)

		b.Complete = client.NewJob("batchDone", 1, "string", 3)
		b.Description = "Test batch"

		err := b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)
			err = b.Push(client.NewJob("JobTwo", 2))
			assert.Nil(t, err)
			return nil
		})
		assert.Nil(t, err)
		assert.NotEqual(t, "", b.Bid)

		time.Sleep(1 * time.Second)
		batchData, err := batchSystem.batchManager.getBatch(b.Bid)
		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Total)

		// job one
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, 0, batchData.Meta.Succeeded)
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
		assert.False(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))

		// job two
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, batchData.Meta.Succeeded, 1)
			assert.Equal(t, batchData.Meta.Failed, 0)
		})

		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Succeeded)
		assert.Equal(t, 0, batchData.Meta.Failed)
		assert.True(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))

		assert.Equal(t, "1", batchData.Meta.CompleteJobState)
		// completeJob
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, "batchDone", job.Type)
		})
		assert.Nil(t, err)
		assert.Equal(t, "2", batchData.Meta.CompleteJobState)

		fetchedJob, err := cl.Fetch("default")
		assert.Nil(t, fetchedJob)

		_, err = batchSystem.batchManager.getBatch(b.Bid)

		// ensure batch is removed
		assert.Error(t, err)
		assert.EqualError(t, err, "getBatch: no batch found")
	})
}

func TestBatchCompleteAndEventualSuccess(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, true, func(cl *client.Client) {
		b := client.NewBatch(cl)

		b.Complete = client.NewJob("batchDone", 1, "string", 3)
		b.Success = client.NewJob("batchSuccess", 2, "string", 4)

		err := b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)
			jobTwo := client.NewJob("JobTwo", 2)
			jobTwo.Retry = 2
			err = b.Push(jobTwo)
			assert.Nil(t, err)
			return nil
		})
		assert.Nil(t, err)
		assert.NotEqual(t, "", b.Bid)

		batchData, err := batchSystem.batchManager.getBatch(b.Bid)
		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Total)
		assert.Equal(t, 2, batchData.Meta.Pending)

		// job one
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, 0, batchData.Meta.Succeeded)
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
		assert.False(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))
		assert.Equal(t, 1, batchData.Meta.Pending)

		// job two
		err = processJob(cl, false, func(job *client.Job) {
			assert.Equal(t, batchData.Meta.Succeeded, 1)
			assert.Equal(t, batchData.Meta.Failed, 0)
		})

		assert.Nil(t, err)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
		assert.Equal(t, 1, batchData.Meta.Failed)
		assert.Equal(t, 0, batchData.Meta.Pending)
		assert.True(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))

		// done job
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, "batchDone", job.Type)
		})
		assert.Nil(t, err)
		assert.Equal(t, "", batchData.Meta.SuccessJobState)
		fetchedJob, err := cl.Fetch("default")
		assert.Nil(t, err)
		assert.Nil(t, fetchedJob)

		_, err = batchSystem.Server.Manager().RetryJobs(time.Now().Add(60 * time.Second))
		assert.Nil(t, err)

		// job two retry #1 fail
		err = processJob(cl, false, func(job *client.Job) {
			assert.Equal(t, "JobTwo", job.Type)
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
		assert.Equal(t, 2, batchData.Meta.Failed)
		assert.Equal(t, 0, batchData.Meta.Pending)

		_, err = batchSystem.Server.Manager().RetryJobs(time.Now().Add(60 * time.Second))
		assert.Nil(t, err)

		// job two retry #2 success
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, "JobTwo", job.Type)
		})
		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Succeeded)
		assert.Equal(t, 2, batchData.Meta.Failed)
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, "batchSuccess", job.Type)
		})
		fetchedJob, err = cl.Fetch("default")
		assert.Nil(t, err)
		assert.Nil(t, fetchedJob)
	})
}

func TestBatchReopen(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, true, func(cl *client.Client) {
		b := client.NewBatch(cl)

		b.Success = client.NewJob("batchSuccess", 2, "string", 4)

		err := b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)
			return nil
		})
		assert.Nil(t, err)
		assert.NotEqual(t, "", b.Bid)

		batchData, err := batchSystem.batchManager.getBatch(b.Bid)
		assert.True(t, batchData.Meta.Committed)

		// job one
		err = processJob(cl, true, func(job *client.Job) {
			b, err = cl.BatchOpen(b.Bid)
			assert.Nil(t, err)
			err = b.Push(client.NewJob("JobTwo", 1))
			assert.Nil(t, err)
			err = b.Push(client.NewJob("JobThree", 2))
			assert.Nil(t, err)
			err = b.Commit()
			assert.Nil(t, err)
		})
		assert.True(t, batchData.Meta.Committed)
		assert.Nil(t, err)

		// job two
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, 1, batchData.Meta.Succeeded)
		})
		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Succeeded)
		assert.False(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))

		// job three
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, 2, batchData.Meta.Succeeded)
			assert.Equal(t, 0, batchData.Meta.Failed)
		})
		assert.Nil(t, err)
		assert.True(t, batchSystem.batchManager.areBatchJobsCompleted(batchData))
	})
}

func TestBatchCannotOpen(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, true, func(cl *client.Client) {
		b := client.NewBatch(cl)

		b.Success = client.NewJob("batchSuccess", 2, "string", 4)

		err := b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)
			return nil
		})
		assert.Nil(t, err)
		assert.NotEqual(t, "", b.Bid)

		batchData, err := batchSystem.batchManager.getBatch(b.Bid)
		assert.True(t, batchData.Meta.Committed)

		// job one
		err = processJob(cl, true, nil)
		assert.True(t, batchData.Meta.Committed)
		assert.Nil(t, err)

		b, err = cl.BatchOpen(b.Bid)
		assert.Error(t, err)
		assert.EqualError(t, err, "ERR batch has already finished")
	})
}

func TestBatchLoadBatches(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, true, func(cl *client.Client) {
		b := client.NewBatch(cl)

		b.Success = client.NewJob("batchSuccess", 2, "string", 4)

		err := b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)

			err = b.Push(client.NewJob("JobTwo", 1))
			assert.Nil(t, err)

			err = b.Push(client.NewJob("JobThree", 1))
			assert.Nil(t, err)

			err = b.Push(client.NewJob("JobFour", 1))
			assert.Nil(t, err)
			return nil
		})
		assert.Nil(t, err)

		processJob(cl, true, nil)
		processJob(cl, false, nil)
		batchSystem.batchManager.Batches = make(map[string]*batch)
		_, err = batchSystem.batchManager.getBatch(b.Bid)
		assert.EqualError(t, err, "getBatch: no batch found")
		err = batchSystem.batchManager.loadExistingBatches()
		assert.Nil(t, err)
		batchData, err := batchSystem.batchManager.getBatch(b.Bid)
		assert.Nil(t, err)
		assert.Equal(t, 4, batchData.Meta.Total)
		assert.Equal(t, 1, batchData.Meta.Failed)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
	})
}

func TestBatchOptions(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, false, func(cl *client.Client) {
		assert.False(t, batchSystem.Options.Enabled)
		assert.Nil(t, batchSystem.Server)
	})
}

func TestBatchBatchWithoutCallbacks(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, true, func(cl *client.Client) {
		b := client.NewBatch(cl)

		err := b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)

			err = b.Push(client.NewJob("JobTwo", 1))
			assert.Nil(t, err)

			err = b.Push(client.NewJob("JobThree", 1))
			assert.Nil(t, err)

			err = b.Push(client.NewJob("JobFour", 1))
			assert.Nil(t, err)
			return nil
		})
		assert.Error(t, err)
		assert.EqualError(t, err, "cannot create new batch: ERR success and/or a complete job callback must be included in batch creation")
	})
}

func TestRemoveStaleBatches(t *testing.T) {
	batchSystem := new(BatchSubsystem)

	withServer(batchSystem, true, func(cl *client.Client) {
		committedBatchId := fmt.Sprintf("b-%s", util.RandomJid())
		meta := batchSystem.batchManager.newBatchMeta("testing", "", "", nil)
		meta.CreatedAt = time.Now().UTC().Add(-time.Duration(1)*time.Minute).AddDate(0, 0, -batchSystem.Options.CommittedTimeoutDays).Format(time.RFC3339Nano)
		batch, err := batchSystem.batchManager.newBatch(committedBatchId, meta)
		assert.Nil(t, err)
		err = batchSystem.batchManager.commit(batch)
		assert.Nil(t, err)

		uncommittedBatchId := fmt.Sprintf("b-%s", util.RandomJid())
		uncommittedMeta := batchSystem.batchManager.newBatchMeta("testing", "", "", nil)
		uncommittedMeta.CreatedAt = time.Now().UTC().Add(-time.Duration(batchSystem.Options.UncommittedTimeoutMinutes+1) * time.Minute).UTC().Format(time.RFC3339Nano)
		_, err = batchSystem.batchManager.newBatch(uncommittedBatchId, uncommittedMeta)
		assert.Nil(t, err)

		batchSystem.batchManager.removeStaleBatches()

		_, err = batchSystem.batchManager.getBatch(committedBatchId)
		assert.EqualError(t, err, "getBatch: no batch found")

		_, err = batchSystem.batchManager.getBatch(uncommittedBatchId)
		assert.EqualError(t, err, "getBatch: no batch found")
	})
}

func withServer(batchSystem *BatchSubsystem, enabled bool, runner func(cl *client.Client)) {
	dir := "/tmp/batching_test.db"
	defer os.RemoveAll(dir)
	opts := &cli.CliOptions{"localhost:7416", "localhost:7420", "development", ".", "debug", dir}
	s, stopper, err := cli.BuildServer(opts)

	if err != nil {
		panic(err)
	}
	defer stopper()
	defer s.Stop(nil)

	go cli.HandleSignals(s)

	err = s.Boot()
	if err != nil {
		panic(err)
	}

	s.Options.GlobalConfig["batch"] = map[string]interface{}{
		"enabled": enabled,
	}

	s.Register(batchSystem)

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()

	cl, err := getClient()
	defer cl.Close()
	if err != nil {
		panic(err)
	}

	runner(cl)
}

func getClient() (*client.Client, error) {
	// this is a worker process so we need to set the global WID before connecting
	client.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)

	srv := client.DefaultServer()
	srv.Address = "localhost:7416"
	cl, err := client.Dial(srv, "123456")
	if err != nil {
		return nil, err
	}
	if _, err = cl.Beat(); err != nil {
		return nil, err
	}

	return cl, nil
}

func processJob(cl *client.Client, success bool, runner func(job *client.Job)) error {
	fetchedJob, err := cl.Fetch("default")
	if err != nil {
		return err
	}

	if runner != nil {
		runner(fetchedJob)
	}

	if fetchedJob == nil {
		return nil
	}
	if success {
		if err := cl.Ack(fetchedJob.Jid); err != nil {
			return err
		}
	} else {
		if err := cl.Fail(fetchedJob.Jid, errors.New("failed"), nil); err != nil {
			return err
		}
	}
	return nil
}
