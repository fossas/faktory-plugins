package batch

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
)

func TestBatchSuccess(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, func(cl *client.Client) {
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
		batchData, err := batchSystem.getBatch(b.Bid)
		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Total)

		// job one
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, 0, batchData.Meta.Succeeded)
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
		assert.False(t, batchData.isBatchDone())

		// job two
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, batchData.Meta.Succeeded, 1)
			assert.Equal(t, batchData.Meta.Failed, 0)
		})

		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Succeeded)
		assert.Equal(t, 0, batchData.Meta.Failed)
		assert.True(t, batchData.isBatchDone())

		assert.Equal(t, "1", batchData.Meta.CompleteJobState)
		assert.Equal(t, "1", batchData.Meta.SuccessJobState)
		// successJob
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, "batchSuccess", job.Type)
		})
		assert.Nil(t, err)
		assert.Equal(t, "2", batchData.Meta.SuccessJobState)

		// completeJob
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, "batchDone", job.Type)
		})
		assert.Nil(t, err)
		assert.Equal(t, "2", batchData.Meta.CompleteJobState)
		fetchedJob, err := cl.Fetch("default")
		assert.Nil(t, fetchedJob)
	})
}

func TestBatchComplete(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, func(cl *client.Client) {
		b := client.NewBatch(cl)

		b.Complete = client.NewJob("batchDone", 1, "string", 3)
		b.Success = client.NewJob("batchSuccess", 2, "string", 4)

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
		batchData, err := batchSystem.getBatch(b.Bid)
		assert.Nil(t, err)
		assert.Equal(t, 2, batchData.Meta.Total)

		// job one
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, 0, batchData.Meta.Succeeded)
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
		assert.False(t, batchData.isBatchDone())

		// job two
		err = processJob(cl, false, func(job *client.Job) {
			assert.Equal(t, batchData.Meta.Succeeded, 1)
			assert.Equal(t, batchData.Meta.Failed, 0)
		})

		assert.Nil(t, err)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
		assert.Equal(t, 1, batchData.Meta.Failed)
		assert.True(t, batchData.isBatchDone())

		// done job
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, "batchDone", job.Type)
		})
		assert.Nil(t, err)

		fetchedJob, err := cl.Fetch("default")
		assert.Nil(t, fetchedJob)
	})
}

func TestBatchReopen(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, func(cl *client.Client) {
		b := client.NewBatch(cl)

		b.Success = client.NewJob("batchSuccess", 2, "string", 4)

		err := b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)
			return nil
		})
		assert.Nil(t, err)
		assert.NotEqual(t, "", b.Bid)

		time.Sleep(1 * time.Second)
		batchData, err := batchSystem.getBatch(b.Bid)
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
		assert.False(t, batchData.isBatchDone())

		// job three
		err = processJob(cl, true, func(job *client.Job) {
			assert.Equal(t, 2, batchData.Meta.Succeeded)
			assert.Equal(t, 0, batchData.Meta.Failed)
		})
		assert.Nil(t, err)
		assert.True(t, batchData.isBatchDone())
	})
}

func TestBatchCannotOpen(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, func(cl *client.Client) {
		b := client.NewBatch(cl)

		b.Success = client.NewJob("batchSuccess", 2, "string", 4)

		err := b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)
			return nil
		})
		assert.Nil(t, err)
		assert.NotEqual(t, "", b.Bid)

		time.Sleep(1 * time.Second)
		batchData, err := batchSystem.getBatch(b.Bid)
		assert.True(t, batchData.Meta.Committed)

		// job one
		err = processJob(cl, true, nil)
		assert.True(t, batchData.Meta.Committed)
		assert.Nil(t, err)

		b, err = cl.BatchOpen(b.Bid)
		assert.Error(t, err)
		assert.EqualError(t, err, "ERR Batch has already finished")
	})
}

func TestBatchInvalidWorkerOpen(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, func(cl *client.Client) {
		otherClient, err := getClient()
		if err != nil {
			panic(err)
		}
		b := client.NewBatch(cl)

		b.Success = client.NewJob("batchSuccess", 2, "string", 4)

		err = b.Jobs(func() error {
			err := b.Push(client.NewJob("JobOne", 1))
			assert.Nil(t, err)
			return nil
		})
		assert.Nil(t, err)
		assert.NotEqual(t, "", b.Bid)

		time.Sleep(1 * time.Second)
		batchData, err := batchSystem.getBatch(b.Bid)
		assert.True(t, batchData.Meta.Committed)

		// job one

		b, err = otherClient.BatchOpen(b.Bid)
		assert.Error(t, err)
		assert.EqualError(t, err, "ERR This worker is not working on a job in the requested batch")
	})
}

func TestBatchLoadBatches(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	withServer(batchSystem, func(cl *client.Client) {
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
		batchSystem.Batches = make(map[string]*batch)
		_, err = batchSystem.getBatch(b.Bid)
		assert.EqualError(t, err, "No batch found")
		err = batchSystem.loadExistingBatches()
		assert.Nil(t, err)
		batchData, err := batchSystem.getBatch(b.Bid)
		assert.Nil(t, err)
		assert.Equal(t, 4, batchData.Meta.Total)
		assert.Equal(t, 1, batchData.Meta.Failed)
		assert.Equal(t, 1, batchData.Meta.Succeeded)
	})
}

func withServer(batchSystem *BatchSubsystem, runner func(cl *client.Client)) {
	dir := "/tmp/system.db"
	defer os.RemoveAll(dir)
	opts := &cli.CliOptions{"localhost:7419", "localhost:7420", "development", ".", "debug", dir}
	s, stopper, err := cli.BuildServer(opts)

	defer s.Stop(nil)
	if stopper != nil {
		defer stopper()
	}
	if err != nil {
		panic(err)
	}

	go cli.HandleSignals(s)

	err = s.Boot()
	if err != nil {
		panic(err)
	}

	s.Register(batchSystem)

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()

	// client.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)

	// cl, err := client.Dial(client.DefaultServer(), "123456")
	// if err != nil {
	// 	panic(err)
	// }
	// defer cl.Close()
	// if _, err = cl.Beat(); err != nil {
	// 	panic(err)
	// }
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

	cl, err := client.Dial(client.DefaultServer(), "123456")
	if err != nil {
		return nil, err
	}
	if _, err = cl.Beat(); err != nil {
		return nil, err
	}

	return cl, nil
}

func handleError(err error) {
	fmt.Println(strings.Replace(err.Error(), "\n", "", -1))
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
