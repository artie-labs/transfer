package dwh

import (
	"context"
	"github.com/artie-labs/transfer/lib/logger"
)

const dwhKey = "_dwh"

func InjectDwhIntoCtx(dwh DataWarehouse, ctx context.Context) context.Context {
	return context.WithValue(ctx, dwhKey, dwh)
}

func FromContext(ctx context.Context) DataWarehouse {
	dwhVal := ctx.Value(dwhKey)
	if dwhVal == nil {
		logger.FromContext(ctx).Fatal("dwh missing from context")
	}

	dwh, isOk := dwhVal.(DataWarehouse)
	if !isOk {
		logger.FromContext(ctx).WithField("dwhVal", dwhVal).Fatal("dwh type is incorrect")
	}

	return dwh
}
