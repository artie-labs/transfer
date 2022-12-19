package metrics

import (
	"context"
)

const metricsClientKey = "_mck"

func InjectMetricsClientIntoCtx(ctx context.Context, metricsClient MetricsClient) context.Context {
	return context.WithValue(ctx, metricsClientKey, metricsClient)
}

func FromContext(ctx context.Context) MetricsClient {
	metricsClientVal := ctx.Value(metricsClientKey)
	if metricsClientVal == nil {
		// TODO: Test.
		return NullMetricsProvider{}
	}

	metricsClient, isOk := metricsClientVal.(MetricsClient)
	if !isOk {
		return NullMetricsProvider{}
	}

	return metricsClient
}
