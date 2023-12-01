package base

import "time"

type Client interface {
	Timing(name string, value time.Duration, tags map[string]string)
	Incr(name string, tags map[string]string)
	Count(name string, value int64, tags map[string]string)
	Gauge(name string, value float64, tags map[string]string)
	GaugeWithSample(name string, value float64, tags map[string]string, sample float64)
}
