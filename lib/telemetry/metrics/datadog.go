package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/DataDog/datadog-go/statsd"
)

type statsClient struct {
	client *statsd.Client
	rate   float64
}

func NewDatadogClient(ctx context.Context, settings map[string]interface{}) (context.Context, error) {
	// TODO: template
	datadogClient, err := statsd.New("127.0.0.1:8125")
	if err != nil {
		return ctx, err
	}

	// This is the default namespace.
	datadogClient.Namespace = "transfer."

	ctx = InjectMetricsClientIntoCtx(ctx, &statsClient{
		client: datadogClient,
		// TODO: allow sampling later.
		rate: 1,
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
