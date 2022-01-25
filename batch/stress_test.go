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
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestBatchStress(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	dir := "/tmp/batching_stress_test.db"
	defer os.RemoveAll(dir)
	opts := &cli.CliOptions{"localhost:7416", "localhost:7420", "development", ".", "debug", dir}
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

	batches := 5
	depth := 12
	jobsPerBatch := 10
	waitGroups := 3
	total := (batches * depth * jobsPerBatch * jobsPerBatch) + batches
	var wg sync.WaitGroup
	for i := 0; i < waitGroups; i++ {
		wg.Add(1)
		cl, err := getClient()
		assert.Nil(t, err)
		go func(queue string, cl *client.Client) {
			createAndProcessBatches(cl, batches, depth, jobsPerBatch, queue)
			log.Println(fmt.Sprintf("Processed %d total jobs and batches (count=%d, children=%d, depth=%d) in %v", total, batches, jobsPerBatch, depth, time.Since(start)))
			cl.Close()
			wg.Done()
		}(fmt.Sprintf("default-%d", i), cl)
	}
	wg.Wait()
	currentCount := 0
	assert.EqualValues(t, total*waitGroups, int(s.Store().TotalProcessed()))
	for i := 0; i < waitGroups; i++ {
		q, err := s.Store().GetQueue(fmt.Sprintf("default-%d", i))
		assert.Nil(t, err)
		currentCount += int(q.Size())
	}

	assert.EqualValues(t, 0, currentCount)
	batchQueue, err := s.Store().GetQueue("batch_load")
	assert.Nil(t, err)
	assert.EqualValues(t, 2*waitGroups*total, int(batchQueue.Size()))
	s.Stop(nil)
}

func runJob(cl *client.Client, jobsPerBatch int, depth int, currentDepth int, queue string) func(*client.Job) {
	return func(job *client.Job) {
		if currentDepth == depth {
			goto done
		}
		for i := 0; i < jobsPerBatch; i++ {
			childBatch, err := createBatch(cl, 1, queue)

			if err != nil {
				fmt.Println(err)
				return
			}

			if _, err = cl.Generic(fmt.Sprintf("BATCH CHILD %s %s", job.Custom["bid"], childBatch.Bid)); err != nil {
				fmt.Println(fmt.Errorf("cannot add child: %v", err))
				return
			}
		}
	done:
		if _, err := cl.Generic(fmt.Sprintf("BATCH COMMIT %s", job.Custom["bid"])); err != nil {
			fmt.Println(fmt.Errorf("cannot commit batch: %v", err))
			return
		}
	}
}

func createAndProcessBatches(cl *client.Client, count int, depth int, jobsPerBatch int, queue string) {
	now := time.Now()
	for i := 0; i < count; i++ {
		if time.Since(now) > 13*time.Second {
			now = time.Now()
			if _, err := cl.Beat(); err != nil {
				fmt.Println(fmt.Sprintf("error beat: %v", err))
			}
		}
		// first batch
		_, err := createBatch(cl, 1, queue)
		if err != nil {
			fmt.Println(fmt.Errorf("unable to create batch: %v", err))
			return
		}
		// for each depth, create x jobs
		for d := 0; d < depth; d++ {
			for j := 0; j < jobsPerBatch; j++ {
				if err = processJobForBatch(cl, queue, runJob(cl, jobsPerBatch, depth, d, queue)); err != nil {
					fmt.Println(err)
					return
				}
			}
		}
	}
	if _, err := cl.Beat(); err != nil {
		fmt.Println(fmt.Sprintf("error beat: %v", err))
	}
	now = time.Now()
	currentCount := count * depth * jobsPerBatch
	total := (count * depth * jobsPerBatch * jobsPerBatch) - currentCount + count
	for i := 0; i < total; i++ {
		if time.Since(now) > 13*time.Second {
			now = time.Now()
			if _, err := cl.Beat(); err != nil {
				fmt.Println(fmt.Sprintf("error beat: %v", err))
			}
		}
		if err := processJobForBatch(cl, queue, runJob(cl, jobsPerBatch, depth, depth, queue)); err != nil {
			fmt.Println(err)
			return
		}
	}
}

func createBatch(cl *client.Client, jobsPerBatch int, queue string) (*client.Batch, error) {
	b := client.NewBatch(cl)
	successJob := client.NewJob("batchSuccess", 1, "string", 2)
	successJob.Queue = "batch_load"
	b.Success = successJob
	completeJob := client.NewJob("batchComplete", 2, "string", 3)
	completeJob.Queue = "batch_load"
	b.Complete = completeJob
	if _, err := cl.BatchNew(b); err != nil {
		return nil, fmt.Errorf("cannot create new batch: %v", err)
	}
	for x := 0; x < jobsPerBatch; x++ {
		if err := pushJob(b, queue); err != nil {
			fmt.Println(fmt.Errorf("error pushing job %s: %v", b.Bid, err))
		}
	}
	return b, nil
}

func pushJob(b *client.Batch, queue string) error {
	job := client.NewJob("SomeJob", 1)
	job.Queue = queue
	return b.Push(job)
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

func processJobForBatch(cl *client.Client, queue string, runner func(job *client.Job)) error {
	fetchedJob, err := cl.Fetch(queue)
	if err != nil {
		return fmt.Errorf("unable to fetch job: %v", err)
	}

	if runner != nil {
		runner(fetchedJob)
	}

	if fetchedJob == nil {
		return nil
	}
	if err := cl.Ack(fetchedJob.Jid); err != nil {
		return fmt.Errorf("unable to ack job: %v", err)
	}
	return nil
}
