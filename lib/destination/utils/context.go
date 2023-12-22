package utils

import (
	"context"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/logger"
)

func InjectDwhIntoCtx(dwh destination.DataWarehouse, ctx context.Context) context.Context {
	return context.WithValue(ctx, constants.DestinationKey, dwh)
}

func InjectBaselineIntoCtx(fs destination.Baseline, ctx context.Context) context.Context {
	return context.WithValue(ctx, constants.DestinationKey, fs)
}

func FromContext(ctx context.Context) destination.Baseline {
	destVal := ctx.Value(constants.DestinationKey)
	if destVal == nil {
		logger.FromContext(ctx).Fatal("destination missing from context")
	}

	// Check if the key is a type destination.DataWarehouse or destination.Baseline
	baseline, isOk := destVal.(destination.Baseline)
	if isOk {
		return baseline
	}

	dwh, isOk := destVal.(destination.DataWarehouse)
	if !isOk {
		logger.FromContext(ctx).WithField("dwhVal", destVal).Fatal("destination type is incorrect")
	}

	return dwh
}
