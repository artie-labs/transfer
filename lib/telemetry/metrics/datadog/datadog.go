package datadog

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/stringutil"
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

// getSampleRate will first parse the val to get a float
// Then it will check if float is a valid sample rate.
// If it's invalid, it will return the default sample, else the passed in rate
func getSampleRate(val interface{}) float64 {
	floatVal, err := strconv.ParseFloat(fmt.Sprint(val), 64)
	if err != nil {
		return DefaultSampleRate
	}

	if floatVal > 1 || floatVal <= 0 {
		return DefaultSampleRate
	}

	return floatVal
}

func NewDatadogClient(ctx context.Context, settings map[string]interface{}) (base.Client, error) {
	address := fmt.Sprint(maputil.GetKeyFromMap(settings, DatadogAddr, DefaultAddr))
	host := os.Getenv("TELEMETRY_HOST")
	port := os.Getenv("TELEMETRY_PORT")
	if !stringutil.Empty(host, port) {
		address = fmt.Sprintf("%s:%s", host, port)
		logger.FromContext(ctx).WithField("address", address).Info("overriding telemetry address with env vars")
	}

	datadogClient, err := statsd.New(address)
	if err != nil {
		return nil, err
	}

	datadogClient.Namespace = fmt.Sprint(maputil.GetKeyFromMap(settings, Namespace, DefaultNamespace))
	datadogClient.Tags = getTags(maputil.GetKeyFromMap(settings, Tags, []string{}))

	return &statsClient{
		client: datadogClient,
		rate:   getSampleRate(maputil.GetKeyFromMap(settings, Sampling, DefaultSampleRate)),
	}, nil
}

type statsClient struct {
	client *statsd.Client
	rate   float64
}

func (s *statsClient) Timing(name string, value time.Duration, tags map[string]string) {
	_ = s.client.Timing(name, value, toDatadogTags(tags), s.rate)
	return
}

func (s *statsClient) Incr(name string, tags map[string]string) {
	_ = s.client.Incr(name, toDatadogTags(tags), s.rate)
	return
}

func (s *statsClient) Count(name string, value int64, tags map[string]string) {
	_ = s.client.Count(name, value, toDatadogTags(tags), s.rate)
}

func (s *statsClient) Gauge(name string, value float64, tags map[string]string) {
	_ = s.client.Gauge(name, value, toDatadogTags(tags), s.rate)
}
