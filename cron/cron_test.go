package cron

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
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
				fmt.Println(job.EntryId)
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
}

func handleError(err error) {
	fmt.Println(strings.Replace(err.Error(), "\n", "", -1))
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
