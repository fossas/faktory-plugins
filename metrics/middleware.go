package metrics

import (
	"context"
	"time"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

func (m *MetricsSubsystem) addMiddleware() {
	m.Server.Manager().AddMiddleware("ack", func(ctx context.Context, next func() error) error {
		mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
		tags := m.getTagsFromJob(mh)

		if mh.Reservation() != nil {
			if err := m.StatsDClient().Timing(m.PrefixMetricName("succeeded.time"), time.Duration(time.Since(mh.Reservation().ReservedAt())), tags, 1); err != nil {
				util.Warnf("unable to submit metric: %v", err)
			}
		}

		return next()
	})

	m.Server.Manager().AddMiddleware("fail", func(ctx context.Context, next func() error) error {
		mh := ctx.Value(manager.MiddlewareHelperKey).(manager.Context)
		tags := m.getTagsFromJob(mh)

		if mh.Reservation() != nil {
			m.StatsDClient().Timing(m.PrefixMetricName("failed.time"), time.Duration(time.Since(mh.Reservation().ReservedAt())), tags, 1)
		}
		return next()
	})
}
