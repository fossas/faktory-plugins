package batch

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/stretchr/testify/assert"
)

func TestBatchStress(t *testing.T) {
	batchSystem := new(BatchSubsystem)
	ctx := context.Background()

	withServer([]server.Subsystem{batchSystem}, enableBatching, func(s *server.Server, cl *client.Client) {
		start := time.Now()

		batches := 5
		depth := 12
		jobsPerBatch := 10
		waitGroups := 3
		total := (batches * depth * jobsPerBatch * jobsPerBatch) + batches
		var wg sync.WaitGroup
		for i := 0; i < waitGroups; i++ {
			wg.Add(1)
			go func(queue string) {
				cl, hb, err := getClient(s.Options.Binding)
				assert.Nil(t, err)
				defer cl.Close()
				defer hb()

				createAndProcessBatches(cl, batches, depth, jobsPerBatch, queue)
				log.Printf("Processed %d total jobs and batches (count=%d, children=%d, depth=%d) in %v\n", total, batches, jobsPerBatch, depth, time.Since(start))
				wg.Done()
			}(fmt.Sprintf("default-%d", i))
		}
		wg.Wait()
		currentCount := 0
		assert.EqualValues(t, 2*total*waitGroups, int(s.Store().TotalProcessed(ctx)))
		for i := 0; i < waitGroups; i++ {
			q, err := s.Store().GetQueue(ctx, fmt.Sprintf("default-%d", i))
			assert.Nil(t, err)
			currentCount += int(q.Size(ctx))
		}

		assert.EqualValues(t, 0, currentCount)
		batchQueue, err := s.Store().GetQueue(ctx, "batch_load_complete")
		assert.Nil(t, err)
		assert.EqualValues(t, waitGroups*total, int(batchQueue.Size(ctx)))
	})
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
				fmt.Printf("error beat: %v\n", err)
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
		fmt.Printf("error beat: %v\n", err)
	}
	now = time.Now()
	currentCount := count * depth * jobsPerBatch
	total := (count * depth * jobsPerBatch * jobsPerBatch) - currentCount + count
	for i := 0; i < total; i++ {
		if time.Since(now) > 13*time.Second {
			now = time.Now()
			if _, err := cl.Beat(); err != nil {
				fmt.Printf("error beat: %v\n", err)
			}
		}
		if err := processJobForBatch(cl, queue, runJob(cl, jobsPerBatch, depth, depth, queue)); err != nil {
			fmt.Println(err)
			return
		}
	}
	for i := 0; i < total+currentCount; i++ {
		if time.Since(now) > 13*time.Second {
			now = time.Now()
			if _, err := cl.Beat(); err != nil {
				fmt.Printf("error beat: %v\n", err)
			}
		}
		if err := processJobForBatch(cl, "batch_load", nil); err != nil {
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
	completeJob.Queue = "batch_load_complete"
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
