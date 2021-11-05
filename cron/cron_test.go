package cron

import (
	"fmt"
	"github.com/fossas/faktory-plugins/cron/mocks"
	"github.com/golang/mock/gomock"
	"github.com/robfig/cron/v3"
	"io/ioutil"
	"os"
	"testing"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/server"
	"github.com/stretchr/testify/assert"
)

const (
	enabledConfig = `
	[cron_plugin]
	  enabled = true
	[[ cron ]]
	  schedule = "* * * * *" # every minute
	  [cron.job]
	    type = "minutely_test_job"
		retry = 5
		queue = "test"
		args = [1, 3]
		[cron.job.custom]
		  testing = true
		
    [[ cron ]]
	  schedule = "* * * * * *" # every second
	  [cron.job]
	    type = "secondly_test_job"
		retry = 1
		queue = "test"
	`

	disabledConfig = `
	[cron_plugin]
	  enabled = false
	`
)

func createConfigDir(t *testing.T) string {
	tmpDir := t.TempDir()
	os.Mkdir(fmt.Sprintf("%s/conf.d", tmpDir), os.FileMode(0777))
	return tmpDir
}

func TestCron(t *testing.T) {
	t.Run("no configuration", func(t *testing.T) {
		system := new(CronSubsystem)
		configDir := createConfigDir(t)
		runSystem(configDir, func(s *server.Server) {
			system.Start(s)
			assert.False(t, system.Options.Enabled)
			assert.Nil(t, system.Cron)
		})
	})

	t.Run("plugin is disabled", func(t *testing.T) {
		system := new(CronSubsystem)
		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/cron.toml", configDir)
		if err := ioutil.WriteFile(confgFile, []byte(disabledConfig), os.FileMode(0444)); err != nil {
			panic(err)
		}
		runSystem(configDir, func(s *server.Server) {
			err := system.Start(s)
			assert.Nil(t, err)
			assert.False(t, system.Options.Enabled)
			assert.Nil(t, system.Cron)
		})
	})

	t.Run("invalid cron job", func(t *testing.T) {
		system := new(CronSubsystem)
		configDir := createConfigDir(t)
		runSystem(configDir, func(s *server.Server) {
			s.Options.GlobalConfig["cron_plugin"] = map[string]interface{}{
				"enabled": true,
			}

			cronJob := map[string]interface{}{
				"schedule": "*",
				"job": map[string]interface{}{
					"type": "test_job",
				},
			}
			cronConfig := []map[string]interface{}{cronJob}
			s.Options.GlobalConfig["cron"] = cronConfig
			err := system.Start(s)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Unable to start cron plugin")
		})
		runSystem(configDir, func(s *server.Server) {
			s.Options.GlobalConfig["cron_plugin"] = map[string]interface{}{
				"enabled": true,
			}

			cronJob := map[string]interface{}{
				"job": map[string]interface{}{
					"type": "test_job",
				},
			}
			cronConfig := []map[string]interface{}{cronJob}
			s.Options.GlobalConfig["cron"] = cronConfig
			err := system.Start(s)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "schedule missing from cronjob")
		})
		runSystem(configDir, func(s *server.Server) {
			s.Options.GlobalConfig["cron_plugin"] = map[string]interface{}{
				"enabled": true,
			}

			cronJob := map[string]interface{}{
				"schedule": "* * * * * *",
			}
			cronConfig := []map[string]interface{}{cronJob}
			s.Options.GlobalConfig["cron"] = cronConfig
			err := system.Start(s)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "job missing from cronjob")
		})
		runSystem(configDir, func(s *server.Server) {
			s.Options.GlobalConfig["cron_plugin"] = map[string]interface{}{
				"enabled": true,
			}

			cronJob := map[string]interface{}{
				"schedule": "* * * * * *",
				"job": map[string]interface{}{
					"queue": "Test",
				},
			}
			cronConfig := []map[string]interface{}{cronJob}
			s.Options.GlobalConfig["cron"] = cronConfig
			err := system.Start(s)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "type is required")
		})
		runSystem(configDir, func(s *server.Server) {
			s.Options.GlobalConfig["cron_plugin"] = map[string]interface{}{
				"enabled": true,
			}

			cronJob := map[string]interface{}{
				"schedule": "* * * * * *",
				"job": map[string]interface{}{
					"type": 1,
				},
			}
			cronConfig := []map[string]interface{}{cronJob}
			s.Options.GlobalConfig["cron"] = cronConfig
			err := system.Start(s)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "arseCronJob: unable to unmarshal job")
		})
	})

	t.Run("jobs are added to cron", func(t *testing.T) {
		system := new(CronSubsystem)
		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/cron.toml", configDir)
		if err := ioutil.WriteFile(confgFile, []byte(enabledConfig), os.FileMode(0444)); err != nil {
			panic(err)
		}
		runSystem(configDir, func(s *server.Server) {
			err := system.Start(s)
			assert.Nil(t, err)
			assert.True(t, system.Options.Enabled)
			assert.Len(t, system.Options.CronJobs, 2)
			assert.Len(t, system.Cron.Entries(), 2)
			for _, job := range system.Options.CronJobs {
				assert.NotNil(t, job.EntryId)
			}
		})
	})

	t.Run("reload removes old jobs", func(t *testing.T) {
		system := new(CronSubsystem)
		configDir := createConfigDir(t)
		confgFile := fmt.Sprintf("%s/conf.d/cron.toml", configDir)
		if err := ioutil.WriteFile(confgFile, []byte(enabledConfig), os.FileMode(0444)); err != nil {
			panic(err)
		}
		runSystem(configDir, func(s *server.Server) {
			err := system.Start(s)
			assert.Nil(t, err)
			cronConfig := []map[string]interface{}{}
			s.Options.GlobalConfig["cron"] = cronConfig
			system.Reload(s)
			assert.Len(t, system.Options.CronJobs, 0)
			assert.Len(t, system.Cron.Entries(), 0)
		})
	})

	t.Run("reload adds new ones", func(t *testing.T) {
		system := new(CronSubsystem)
		configDir := createConfigDir(t)
		runSystem(configDir, func(s *server.Server) {
			err := system.Start(s)
			assert.Nil(t, err)
			s.Options.GlobalConfig["cron_plugin"] = map[string]interface{}{
				"enabled": true,
			}

			cronJob := map[string]interface{}{
				"schedule": "* * * * *",
				"job": map[string]interface{}{
					"type": "test_job",
				},
			}
			cronConfig := []map[string]interface{}{cronJob}
			s.Options.GlobalConfig["cron"] = cronConfig
			system.Reload(s)
			assert.Len(t, system.Options.CronJobs, 1)
			assert.Len(t, system.Cron.Entries(), 1)
			assert.Equal(t, system.Options.CronJobs[0].EntryId, system.Cron.Entries()[0].ID)
		})
	})

	t.Run("creates multiple cron jobs", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockDoer := mocks.NewMockCronInterface(mockCtrl)
		system := new(CronSubsystem)
		configDir := createConfigDir(t)

		runSystem(configDir, func(s *server.Server) {
			cronJobOne := map[string]interface{}{
				"schedule": "* * * * *",
				"job": map[string]interface{}{
					"type": "test_job",
				},
			}
			cronJobTwo := map[string]interface{}{
				"schedule": "* * * * * *",
				"job": map[string]interface{}{
					"type": "test_job_2",
				},
			}
			cronConfig := []map[string]interface{}{cronJobOne, cronJobTwo}
			s.Options.GlobalConfig["cron"] = cronConfig
			opts, err := system.getOptions(s)
			assert.Nil(t, err)
			system.Options = opts
			system.Cron = mockDoer
			mockDoer.EXPECT().Start().Times(1)
			mockDoer.EXPECT().
				AddJob("* * * * *", gomock.Any()).
				Return(cron.EntryID(0), nil).
				Times(1).
				Do(func (spec string, job *QueueJob) {
					assert.Equal(t, "test_job", job.job.Name)
				})
			mockDoer.EXPECT().
				AddJob("* * * * * *", gomock.Any()).
				Return(cron.EntryID(1), nil).
				Times(1).
				Do(func (spec string, job *QueueJob) {
					assert.Equal(t, "test_job_2", job.job.Name)
				})
			err = system.addCronJobs()
			assert.Nil(t, err)

		})
	})
}

func runSystem(configDir string, runner func(s *server.Server)) {
	dir := "/tmp/batching_system.db"
	defer os.RemoveAll(dir)
	opts := &cli.CliOptions{"localhost:7417", "localhost:7420", "development", configDir, "debug", dir}
	s, stopper, err := cli.BuildServer(opts)

	defer s.Stop(nil)
	if err != nil {
		panic(err)
	}
	defer stopper()

	go cli.HandleSignals(s)

	err = s.Boot()
	if err != nil {
		panic(err)
	}

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()

	runner(s)
}
