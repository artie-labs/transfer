package logger

import (
	"context"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/sirupsen/logrus"
)

func InjectLoggerIntoCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, constants.LoggerKey, new(config.FromContext(ctx)))
}

func FromContext(ctx context.Context) *logrus.Logger {
	logVal := ctx.Value(constants.LoggerKey)
	if logVal == nil {
		// Inject this back into context, so we don't need to initialize this again
		return FromContext(InjectLoggerIntoCtx(ctx))
	}

	log, isOk := logVal.(*logrus.Logger)
	if !isOk {
		return FromContext(InjectLoggerIntoCtx(ctx))
	}

	return log
}
