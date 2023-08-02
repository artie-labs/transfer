package utils

import (
	"context"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/logger"
)

const dwhKey = "_dwh"

func InjectDwhIntoCtx(dwh destination.DataWarehouse, ctx context.Context) context.Context {
	return context.WithValue(ctx, dwhKey, dwh)
}

func FromContext(ctx context.Context) destination.DataWarehouse {
	dwhVal := ctx.Value(dwhKey)
	if dwhVal == nil {
		logger.FromContext(ctx).Fatal("destination missing from context")
	}

	dwh, isOk := dwhVal.(destination.DataWarehouse)
	if !isOk {
		logger.FromContext(ctx).WithField("dwhVal", dwhVal).Fatal("destination type is incorrect")
	}

	return dwh
}
