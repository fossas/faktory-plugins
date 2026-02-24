package main

import (
	"log"
	"time"

	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/faktory/webui"
	"github.com/fossas/faktory-plugins/batch"
	"github.com/fossas/faktory-plugins/cron"
	"github.com/fossas/faktory-plugins/deadjobs"
	"github.com/fossas/faktory-plugins/expire"
	"github.com/fossas/faktory-plugins/metrics"
	"github.com/fossas/faktory-plugins/requeue"
	"github.com/fossas/faktory-plugins/uniq"
)

var (
	version = "dev"
	commit  = "n/a"
)

func logPreamble() {
	log.SetFlags(0)
	log.Printf("%s %s commit: %s\n", client.Name, version, commit)
	log.Printf("Copyright © %d Contributed Systems LLC and FOSSA Inc\n", time.Now().Year())
	log.Println("Licensed under the GNU Affero Public License 3.0")
}

func main() {
	logPreamble()

	opts := cli.ParseArguments()
	util.InitLogger(opts.LogLevel)
	util.Debugf("Options: %v", opts)

	s, stopper, err := cli.BuildServer(&opts)
	if err != nil {
		util.Error("Unable to create Faktory server", err)
		return
	}
	defer func() { _ = stopper() }()

	err = s.Boot()
	if err != nil {
		util.Error("Unable to boot the command server", err)
		return
	}

	s.Register(webui.Subsystem(opts.WebBinding))

	// fossa plugins
	s.Register(new(batch.BatchSubsystem))
	s.Register(new(uniq.UniqSubsystem))
	s.Register(new(metrics.MetricsSubsystem))
	s.Register(new(cron.CronSubsystem))
	s.Register(new(expire.ExpireSubsystem))
	s.Register(new(requeue.RequeueSubsystem))
	s.Register(new(deadjobs.DeadJobCleanupSubsystem))

	go cli.HandleSignals(s)
	go func() {
		_ = s.Run()
	}()

	<-s.Stopper()
	s.Stop(nil)
}
