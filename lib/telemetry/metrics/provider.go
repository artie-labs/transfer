package metrics

import "time"

type MetricsClient interface {
	Timing(name string, value time.Duration, tags map[string]string)
	Incr(name string, tags map[string]string)
}
