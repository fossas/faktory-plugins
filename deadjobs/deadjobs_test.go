package deadjobs

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	enabledConfig = `
	[dead_job_cleanup]
	enabled = true
	retention_days = 7
	threshold = 5
	batch_size = 100
	interval_seconds = 60
	`

	disabledConfig = `
	[dead_job_cleanup]
	enabled = false
	`

	customConfig = `
	[dead_job_cleanup]
	enabled = true
	retention_days = 30
	threshold = 50000
	batch_size = 5000
	interval_seconds = 7200
	`
)

func createConfigDir(t *testing.T) string {
	tmpDir := t.TempDir()
	os.Mkdir(fmt.Sprintf("%s/conf.d", tmpDir), os.FileMode(0777))
	return tmpDir
}

func writeConfig(t *testing.T, configDir string, config string) {
	t.Helper()
	configFile := fmt.Sprintf("%s/conf.d/dead_job_cleanup.toml", configDir)
	err := os.WriteFile(configFile, []byte(config), os.FileMode(0444))
	require.NoError(t, err)
}

func runSystem(configDir string, runner func(s *server.Server, cl *client.Client)) {
	dir := fmt.Sprintf("/tmp/deadjobs_test_%d.db", rand.Int())
	defer os.RemoveAll(dir)
	opts := &cli.CliOptions{
		CmdBinding:       "localhost:7418",
		Environment:      "development",
		ConfigDirectory:  configDir,
		LogLevel:         "debug",
		StorageDirectory: dir,
	}
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

	go func() {
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}()

	client.RandomProcessWid = strconv.FormatInt(rand.Int63(), 32)

	srv := client.DefaultServer()
	srv.Address = "localhost:7418"
	cl, err := client.Dial(srv, "123456")
	if err != nil {
		panic(err)
	}
	if _, err = cl.Beat(); err != nil {
		panic(err)
	}
	defer cl.Close()

	runner(s, cl)
}

func TestDeadJobCleanup(t *testing.T) {
	t.Run("no configuration", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.False(t, system.Options.Enabled)
		})
	})

	t.Run("plugin is disabled", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, disabledConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.False(t, system.Options.Enabled)
		})
	})

	t.Run("plugin is enabled with defaults", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.True(t, system.Options.Enabled)
			assert.Equal(t, 7, system.Options.RetentionDays)
			assert.Equal(t, 5, system.Options.Threshold)
			assert.Equal(t, 100, system.Options.BatchSize)
			assert.Equal(t, 60, system.Options.IntervalSeconds)
		})
	})

	t.Run("plugin with custom configuration", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, customConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			err := system.Start(s)
			assert.NoError(t, err)
			assert.True(t, system.Options.Enabled)
			assert.Equal(t, 30, system.Options.RetentionDays)
			assert.Equal(t, 50000, system.Options.Threshold)
			assert.Equal(t, 5000, system.Options.BatchSize)
			assert.Equal(t, 7200, system.Options.IntervalSeconds)
		})
	})

	t.Run("cleanup skips when below threshold", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.Options = system.getOptions(s)
			ctx := context.Background()

			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(1), task.sweeps)
			assert.Equal(t, int64(0), task.totalRemoved)
		})
	})

	t.Run("cleanup removes old dead jobs above threshold", func(t *testing.T) {
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, enabledConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.Options = system.getOptions(s)
			ctx := context.Background()

			deadSet := s.Store().Dead()

			// Add 10 dead jobs with timestamps older than retention
			oldTime := time.Now().Add(-14 * 24 * time.Hour) // 14 days ago
			for i := 0; i < 10; i++ {
				job := client.NewJob("OldJob", i)
				job.At = util.Thens(oldTime.Add(time.Duration(i) * time.Minute))
				err := deadSet.Add(ctx, job)
				require.NoError(t, err)
			}

			// Add 3 recent dead jobs
			recentTime := time.Now().Add(-1 * time.Hour)
			for i := 0; i < 3; i++ {
				job := client.NewJob("RecentJob", i)
				job.At = util.Thens(recentTime.Add(time.Duration(i) * time.Minute))
				err := deadSet.Add(ctx, job)
				require.NoError(t, err)
			}

			// Verify initial state
			assert.Equal(t, uint64(13), deadSet.Size(ctx))

			task := &deadJobCleanupTask{subsystem: system}
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(10), task.totalRemoved)
			assert.Equal(t, uint64(3), deadSet.Size(ctx))
		})
	})

	t.Run("cleanup respects batch size limit", func(t *testing.T) {
		batchLimitConfig := `
		[dead_job_cleanup]
		enabled = true
		retention_days = 7
		threshold = 2
		batch_size = 3
		interval_seconds = 60
		`
		system := new(DeadJobCleanupSubsystem)
		configDir := createConfigDir(t)
		writeConfig(t, configDir, batchLimitConfig)
		runSystem(configDir, func(s *server.Server, cl *client.Client) {
			system.Server = s
			system.Options = system.getOptions(s)
			ctx := context.Background()

			deadSet := s.Store().Dead()

			// Add 10 old dead jobs
			oldTime := time.Now().Add(-14 * 24 * time.Hour)
			for i := 0; i < 10; i++ {
				job := client.NewJob("OldJob", i)
				job.At = util.Thens(oldTime.Add(time.Duration(i) * time.Minute))
				err := deadSet.Add(ctx, job)
				require.NoError(t, err)
			}

			task := &deadJobCleanupTask{subsystem: system}

			// First sweep: should remove only 3 (batch_size)
			err := task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(3), task.totalRemoved)
			assert.Equal(t, uint64(7), deadSet.Size(ctx))

			// Second sweep: should remove another 3
			err = task.Execute(ctx)
			assert.NoError(t, err)
			assert.Equal(t, int64(6), task.totalRemoved)
			assert.Equal(t, uint64(4), deadSet.Size(ctx))
		})
	})

	t.Run("stats returns correct values", func(t *testing.T) {
		task := &deadJobCleanupTask{
			sweeps:       5,
			totalRemoved: 42,
		}
		ctx := context.Background()
		stats := task.Stats(ctx)
		assert.Equal(t, int64(5), stats["sweeps"])
		assert.Equal(t, int64(42), stats["total_removed"])
	})

	t.Run("formatCount returns human-readable counts", func(t *testing.T) {
		assert.Equal(t, "0", formatCount(0))
		assert.Equal(t, "999", formatCount(999))
		assert.Equal(t, "1.0K", formatCount(1000))
		assert.Equal(t, "10.5K", formatCount(10500))
		assert.Equal(t, "1.0M", formatCount(1000000))
		assert.Equal(t, "2.5M", formatCount(2500000))
	})
}
