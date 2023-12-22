package metrics

import (
	"context"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
)

func InjectMetricsClientIntoCtx(ctx context.Context, metricsClient base.Client) context.Context {
	return context.WithValue(ctx, constants.MetricsKey, metricsClient)
}

func FromContext(ctx context.Context) base.Client {
	metricsClientVal := ctx.Value(constants.MetricsKey)
	if metricsClientVal == nil {
		return NullMetricsProvider{}
	}

	metricsClient, isOk := metricsClientVal.(base.Client)
	if !isOk {
		return NullMetricsProvider{}
	}

	return metricsClient
}
