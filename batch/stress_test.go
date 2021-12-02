package batch

import (
	"fmt"
	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestBatchStress(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	dir := "/tmp/batching_stress_test.db"
	defer os.RemoveAll(dir)
	opts := &cli.CliOptions{"localhost:7417", "localhost:7420", "development", ".", "debug", dir}
	s, stopper, err := cli.BuildServer(opts)
	if stopper != nil {
		defer stopper()
	}
	if err != nil {
		panic(err)
	}

	go stacks()
	go cli.HandleSignals(s)

	err = s.Boot()
	if err != nil {
		panic(err)
	}

	s.Options.GlobalConfig["batch"] = map[string]interface{}{
		"enabled": true,
	}

	s.Register(batchSystem)

	go func() {
		_ = s.Run()
	}()

	start := time.Now()

	batches := 10
	childBatches := 30
	jobsPerBatch := 25
	total := (batches * childBatches * jobsPerBatch) + (batches * jobsPerBatch)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		cl, err := getClient()
		assert.Nil(t, err)
		go func() {
			defer wg.Done()
			defer cl.Close()
			generateBatchesAndJobs(cl, batches, childBatches, jobsPerBatch)
			processJobs(cl, batches, childBatches, jobsPerBatch)
			log.Println(fmt.Sprintf("Processed %d batches with %d childBatches and %d total jobs in %v", batches, childBatches, total, time.Since(start)))
		}()
	}

	wg.Wait()
	assert.EqualValues(t, total*3, s.Store().TotalProcessed())
	q, err := s.Store().GetQueue("default")
	assert.Nil(t, err)
	assert.EqualValues(t, 0, int(q.Size()))
	batchQueue, err := s.Store().GetQueue("batch_load")
	assert.Nil(t, err)
	assert.EqualValues(t, 2*3*((batches*childBatches)+batches), int(batchQueue.Size()))
	s.Stop(nil)
}

func processJobs(cl *client.Client, count int, depth int, jobsPerBatch int) {
	for i := 0; i < count; i++ {
		for job := 0; job < jobsPerBatch; job++ {
			if err := processJob(cl, true, nil); err != nil {
				handleError(err)
			}
		}
		for x := 0; x < depth; x++ {
			for job := 0; job < jobsPerBatch; job++ {
				if err := processJob(cl, true, nil); err != nil {
					handleError(err)
				}
			}
		}

	}
}

func generateBatchesAndJobs(cl *client.Client, count int, depth int, jobsPerBatch int) {
	for i := 0; i < count; i++ {
		time.Sleep(300 * time.Millisecond)

		parentBatch, err := createBatch(cl, jobsPerBatch)
		if err != nil {
			handleError(err)
			return
		}

		for currentDepth := 0; currentDepth < depth; currentDepth++ {
			newBatch, err := createBatch(cl, jobsPerBatch)
			if err != nil {
				handleError(err)
				return
			}

			_, err = cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", parentBatch.Bid, newBatch.Bid))
			if err != nil {
				handleError(err)
				return
			}
			err = parentBatch.Commit()
			if err != nil {
				handleError(err)
				return
			}

			parentBatch = newBatch
		}
		err = parentBatch.Commit()
		if err != nil {
			handleError(err)
			return
		}
	}
}

func createBatch(cl *client.Client, jobsPerBatch int) (*client.Batch, error) {
	b := client.NewBatch(cl)
	successJob := client.NewJob("batchSuccess", 1, "string", 2)
	successJob.Queue = "batch_load"
	b.Success = successJob
	completeJob := client.NewJob("batchComplete", 2, "string", 3)
	completeJob.Queue = "batch_load"
	b.Complete = completeJob
	_, err := cl.BatchNew(b)
	for x := 0; x < jobsPerBatch; x++ {
		if err := pushJob(b); err != nil {
			handleError(err)
		}
	}

	if err != nil {
		return nil, err
	}
	return b, nil
}

func pushJob(b *client.Batch) error {
	return b.Push(client.NewJob("SomeJob", 1))
}

func handleError(err error) {
	fmt.Println(strings.Replace(err.Error(), "\n", "", -1))
}

func stacks() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	buf := make([]byte, 1<<20)
	for {
		<-sigs
		stacklen := runtime.Stack(buf, true)
		log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
	}
}
