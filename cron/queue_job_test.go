package cron

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/contribsys/faktory/server"
	"github.com/stretchr/testify/assert"
)

func TestQueueJob(t *testing.T) {
	t.Run("faktory jobs are queued", func(t *testing.T) {
		system := new(CronSubsystem)
		ctx := context.Background()

		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/cron.toml", configDir)
		if err := ioutil.WriteFile(confgFile, []byte(enabledConfig), os.FileMode(0444)); err != nil {
			panic(err)
		}
		runSystem(configDir, func(s *server.Server) {
			system.Server = s
			options, err := system.getOptions(s)
			assert.Nil(t, err)
			cronJob := options.CronJobs[0]
			assert.NotNil(t, cronJob)
			queueJob := &QueueJob{
				Subsystem: system,
				job:       &cronJob,
			}
			queueJob.Run()
			queue, err := s.Store().GetQueue(ctx, "test")
			assert.Nil(t, err)
			assert.Equal(t, uint64(1), queue.Size(ctx))

			ctx := context.Background()
			job, err := s.Manager().Fetch(ctx, "", "test")
			assert.Nil(t, err)
			assert.Len(t, job.Args, 2)
			assert.Equal(t, float64(1), job.Args[0])
			assert.Equal(t, float64(3), job.Args[1])

			cronJob = options.CronJobs[1]
			assert.NotNil(t, cronJob)
			queueJob = &QueueJob{
				Subsystem: system,
				job:       &cronJob,
			}

			queueJob.Run()
			jobTwo, err := s.Manager().Fetch(ctx, "", "test")
			assert.Nil(t, err)
			assert.Len(t, jobTwo.Args, 2)
			assert.Equal(t, map[string]interface{}{"hello": "world"}, jobTwo.Args[0])
			assert.Equal(t, map[string]interface{}{"hello": "human"}, jobTwo.Args[1])
		})
	})
}
