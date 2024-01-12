package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

func (m *MetricsSubsystem) addMiddleware() {
	m.Server.Manager().AddMiddleware("push", func(ctx context.Context, next func() error) error {
		mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
		tags := m.getTagsFromJob(mh)

		if err := m.StatsDClient().Incr("faktory.jobs.pushed", tags, float64(1)); err != nil {
			util.Warnf("unable to submit metric: %v", err)
		}

		return next()
	})

	m.Server.Manager().AddMiddleware("fetch", func(ctx context.Context, next func() error) error {
		mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
		tags := m.getTagsFromJob(mh)

		if err := m.StatsDClient().Incr("faktory.jobs.fetched", tags, float64(1)); err != nil {
			util.Warnf("unable to submit metric: %v", err)
		}

		return next()
	})

	m.Server.Manager().AddMiddleware("ack", func(ctx context.Context, next func() error) error {
		mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
		tags := m.getTagsFromJob(mh)
		tags = append(tags, "status:success")

		if mh.Reservation() != nil {
			if err := m.StatsDClient().Timing(m.PrefixMetricName("succeeded.time"), time.Duration(time.Since(mh.Reservation().ReservedAt())), tags, 1); err != nil {
				util.Warnf("unable to submit metric: %v", err)
			}
			if err := m.StatsDClient().Timing("faktory.jobs.processed", time.Duration(time.Since(mh.Reservation().ReservedAt())), tags, 1); err != nil {
				util.Warnf("unable to submit metric: %v", err)
			}
		}

		return next()
	})

	m.Server.Manager().AddMiddleware("fail", func(ctx context.Context, next func() error) error {
		mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
		tags := m.getTagsFromJob(mh)
		// A job is dead and will not be retried if `Retry` is nil or 0, or if
		// `RetryRemaining` is 0.
		dead := (mh.Job().Retry == nil || *mh.Job().Retry == 0) || mh.Job().Failure.RetryRemaining == 0
		tags = append(tags, "status:fail", fmt.Sprintf("dead:%t", dead))

		if mh.Reservation() != nil {
			if err := m.StatsDClient().Timing(m.PrefixMetricName("failed.time"), time.Duration(time.Since(mh.Reservation().ReservedAt())), tags, 1); err != nil {
				util.Warnf("unable to submit metric: %v", err)
			}
			if err := m.StatsDClient().Timing("faktory.jobs.processed", time.Duration(time.Since(mh.Reservation().ReservedAt())), tags, 1); err != nil {
				util.Warnf("unable to submit metric: %v", err)
			}
		}
		return next()
	})
}
