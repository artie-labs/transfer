package metrics

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/DataDog/datadog-go/statsd"

	"github.com/artie-labs/transfer/lib/maputil"
)

const (
	Tags     = "tags"
	Sampling = "sampling"
	// DefaultSampleRate will make sure we do not sample by measuring 100% of our metrics
	DefaultSampleRate = 1

	Namespace = "namespace"
	// DefaultNamespace will be prefixed with "transfer."
	DefaultNamespace = "transfer."

	DatadogAddr = "addr"
	// DefaultAddr is the default address for where the DD agent would be running on a single host machine
	DefaultAddr = "127.0.0.1:8125"
)

type statsClient struct {
	client *statsd.Client
	rate   float64
}

// getSampleRate will first parse the val to get a float
// Then it will check if float is a valid sample rate.
// If it's invalid, it will return the default sample, else the passed in rate
func getSampleRate(val interface{}) float64 {
	floatVal, err := strconv.ParseFloat(fmt.Sprint(val), 64)
	if err != nil {
		return DefaultSampleRate
	}

	if floatVal > 1 || floatVal < 0 {
		return DefaultSampleRate
	}

	return floatVal
}

func getTags(tags interface{}) []string {
	retTags, isOk := tags.([]string)
	if !isOk {
		return nil
	}

	return retTags
}

func NewDatadogClient(ctx context.Context, settings map[string]interface{}) (context.Context, error) {
	datadogClient, err := statsd.New(fmt.Sprint(maputil.GetKeyFromMap(settings, DatadogAddr, DefaultAddr)))
	if err != nil {
		return ctx, err
	}

	datadogClient.Namespace = fmt.Sprint(maputil.GetKeyFromMap(settings, Namespace, DefaultNamespace))
	datadogClient.Tags = getTags(maputil.GetKeyFromMap(settings, Tags, []string{}))

	ctx = InjectMetricsClientIntoCtx(ctx, &statsClient{
		client: datadogClient,
		rate:   getSampleRate(maputil.GetKeyFromMap(settings, Sampling, DefaultSampleRate)),
	})
	return ctx, nil
}

func toDatadogTags(tags map[string]string) []string {
	var retTags []string
	for key, val := range tags {
		retTags = append(retTags, fmt.Sprintf("%s:%s", key, val))
	}

	return retTags
}

func (s *statsClient) Timing(name string, value time.Duration, tags map[string]string) {
	_ = s.client.Timing(name, value, toDatadogTags(tags), s.rate)
	return
}

func (s *statsClient) Incr(name string, tags map[string]string) {
	_ = s.client.Incr(name, toDatadogTags(tags), s.rate)
	return
}
