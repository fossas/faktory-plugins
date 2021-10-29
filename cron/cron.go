package cron

import (
	"encoding/json"
	"fmt"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/util"
	"github.com/robfig/cron/v3"
)

var _ server.Subsystem = &CronSubsystem{}

// Cron Subsystem enables cron jobs to queue faktory jobs
// cron jobs are defined in the configuration
type CronSubsystem struct {
	statsDClient statsd.ClientInterface
	Server       *server.Server
	Options      *Options
	Cron         *cron.Cron
	EntryIDs     []cron.EntryID
}

// Options for the plugin
type Options struct {
	// Enabled controls whether or not the plugin will function
	Enabled bool
	// Tags sent to datdog
	CronJobs []CronJob
}

type CronJob struct {
	EntryId    cron.EntryID
	Name       string
	Schedule   string
	faktoryJob []byte
}

// Start - parse config and schedule cron jobs
func (c *CronSubsystem) Start(s *server.Server) error {
	c.Server = s
	options, err := c.getOptions(s)
	// only throw an error if the config is invalid and the plugin is enabled
	c.Options = options
	if err != nil && c.Options.Enabled {
		return fmt.Errorf("Start: Error parsing config: %v", err)
	}

	if !c.Options.Enabled {
		return nil
	}

	c.createCron()

	if err := c.addCronJobs(); err != nil {
		return fmt.Errorf("Start: cannot add cron jobs: %v", err)
	}

	util.Infof("Started cron plugin, registered %d jobs", len(c.Options.CronJobs))
	go func() {
		// wait for signal and stop cron
		<-c.Server.Stopper()
		c.Cron.Stop()
	}()
	return nil
}

// Name - returns the name of the subsystem or plugin
func (c *CronSubsystem) Name() string {
	return "cron"
}

// Reload - the config is reloaded by faktory
// remove any exist cron jobs and load new ones from the config
func (c *CronSubsystem) Reload(s *server.Server) error {
	options, err := c.getOptions(s)
	// only throw an error if the config is invalid and the plugin is enabled
	if err != nil && c.Options.Enabled {
		return fmt.Errorf("Reload: Error parsing config: %v", err)
	}

	if c.Cron == nil {
		c.createCron()
	} else {
		entries := c.Cron.Entries()
		for _, entry := range entries {
			c.Cron.Remove(entry.ID)
		}
	}

	c.Options = options
	if !c.Options.Enabled {
		return nil
	}

	if err := c.addCronJobs(); err != nil {
		return fmt.Errorf("Reload: cannot add cron jobs: %v", err)
	}
	return nil
}

func (c *CronSubsystem) createCron() {
	c.Cron = cron.New(cron.WithParser(cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)))
}

func (c *CronSubsystem) addCronJobs() error {
	for index, job := range c.Options.CronJobs {
		id, err := c.Cron.AddJob(job.Schedule, &QueueJob{
			Subsystem: c,
			job:       &job,
		})

		if err != nil {
			util.Warnf("Unable to start cron plugin: %v", err)
			return fmt.Errorf("createCron: Unable to start cron plugin: %v", err)
		}
		c.Options.CronJobs[index].EntryId = id
	}

	// starts cron if not already running
	c.Cron.Start()

	return nil
}

func (c *CronSubsystem) getOptions(s *server.Server) (*Options, error) {
	var options Options

	enabledValue := s.Options.Config("cron_plugin", "enabled", false)
	enabled, ok := enabledValue.(bool)
	if !ok {
		enabled = false
	}
	options.Enabled = enabled
	var cronJobs []CronJob

	cronJobsValue, ok := s.Options.GlobalConfig["cron"]
	if !ok {
		return &options, fmt.Errorf("getOptions: no cron jobs provided")
	}

	if cronJobsInterface, ok := cronJobsValue.([]map[string]interface{}); ok {
		for _, cronJobInterface := range cronJobsInterface {
			cronJob, err := c.parseCronJob(cronJobInterface)
			if err != nil {
				util.Warnf("uhoh %v", err)
				return &options, fmt.Errorf("getOptions unable to parse cronjob: %v", err)
			}
			cronJobs = append(cronJobs, cronJob)
		}
	}
	options.CronJobs = cronJobs

	return &options, nil
}

func (c *CronSubsystem) parseCronJob(value map[string]interface{}) (CronJob, error) {
	var cronJob CronJob

	scheduleValue, ok := value["schedule"]
	if !ok {
		return cronJob, fmt.Errorf("parseCronJob: schedule missing from cronjob")
	}
	schedule, ok := scheduleValue.(string)
	if !ok {
		return cronJob, fmt.Errorf("parseCronJob: schedule is not a string")
	}

	cronJob.Schedule = schedule

	jobValue, ok := value["job"]
	if !ok {
		return cronJob, fmt.Errorf("parseCronJob: job missing from cronjob")
	}
	jobInterface, ok := jobValue.(map[string]interface{})
	if !ok {
		return cronJob, fmt.Errorf("parseCronJob: job is not valid")
	}

	if _, ok := jobInterface["args"]; !ok {
		jobInterface["args"] = []interface{}{}
	}

	typeValue, ok := jobInterface["type"]
	if !ok {
		return cronJob, fmt.Errorf("parseCronJob: type is required")
	}
	jobInterface["jobtype"] = typeValue

	// convert job to json
	jobData, err := json.Marshal(jobInterface)
	if err != nil {
		return cronJob, fmt.Errorf("parseCronJob: unable to marshal job: %v", err)
	}

	var job client.Job

	// validate jobData is correct
	err = json.Unmarshal(jobData, &job)

	if err != nil {
		return cronJob, fmt.Errorf("parseCronJob: unable to unmarshal job: %v", err)
	}

	cronJob.faktoryJob = jobData
	cronJob.Name = job.Type
	return cronJob, nil
}
