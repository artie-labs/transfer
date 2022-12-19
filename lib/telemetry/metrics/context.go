package metrics

import (
	"context"
)

const metricsClientKey = "_mck"

func InjectMetricsClientIntoCtx(ctx context.Context, metricsClient Client) context.Context {
	return context.WithValue(ctx, metricsClientKey, metricsClient)
}

func FromContext(ctx context.Context) Client {
	metricsClientVal := ctx.Value(metricsClientKey)
	if metricsClientVal == nil {
		// TODO: Test.
		return NullMetricsProvider{}
	}

	metricsClient, isOk := metricsClientVal.(Client)
	if !isOk {
		return NullMetricsProvider{}
	}

	return metricsClient
}
