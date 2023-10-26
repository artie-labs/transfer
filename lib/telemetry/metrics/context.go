package metrics

import (
	"context"

	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
)

const metricsClientKey = "_mck"

func InjectMetricsClientIntoCtx(ctx context.Context, metricsClient base.Client) context.Context {
	return context.WithValue(ctx, metricsClientKey, metricsClient)
}

func FromContext(ctx context.Context) base.Client {
	metricsClientVal := ctx.Value(metricsClientKey)
	if metricsClientVal == nil {
		return NullMetricsProvider{}
	}

	metricsClient, isOk := metricsClientVal.(base.Client)
	if !isOk {
		return NullMetricsProvider{}
	}

	return metricsClient
}
