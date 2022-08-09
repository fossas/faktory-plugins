package metrics

import (
	"time"

	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/util"
)

func (m *MetricsSubsystem) addMiddleware() {
	m.Server.Manager().AddMiddleware("ack", func(next func() error, ctx manager.Context) error {
		tags := m.getTagsFromJob(ctx)
		if ctx.Reservation() != nil {
			if err := m.StatsDClient().Timing(m.PrefixMetricName("succeeded.time"), time.Duration(time.Now().Sub(ctx.Reservation().ReservedAt())), tags, 1); err != nil {
				util.Warnf("unable to submit metric: %v", err)
			}
		}

		return next()
	})
	m.Server.Manager().AddMiddleware("fail", func(next func() error, ctx manager.Context) error {
		tags := m.getTagsFromJob(ctx)

		if ctx.Reservation() != nil {
			m.StatsDClient().Timing(m.PrefixMetricName("failed.time"), time.Duration(time.Now().Sub(ctx.Reservation().ReservedAt())), tags, 1)
		}
		return next()
	})
}
