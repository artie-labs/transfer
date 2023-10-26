package metrics

import "time"

type NullMetricsProvider struct{}

func (n NullMetricsProvider) Count(name string, value int64, tags map[string]string) {
	return
}

func (n NullMetricsProvider) Timing(name string, value time.Duration, tags map[string]string) {
	return
}

func (n NullMetricsProvider) Incr(name string, tags map[string]string) {
	return
}
