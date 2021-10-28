package cron

import (
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
			job := &QueueJob{
				Subsystem: system,
				job:       &cronJob,
			}
			job.Run()
			queue, err := s.Store().GetQueue("test")
			assert.Nil(t, err)

			assert.Equal(t, uint64(1), queue.Size())
		})

	})
}
