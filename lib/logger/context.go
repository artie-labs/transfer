package logger

import (
	"context"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/sirupsen/logrus"
)

const loggerKey = "_log"

func InjectLoggerIntoCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, loggerKey, new(config.FromContext(ctx)))
}

func FromContext(ctx context.Context) *logrus.Logger {
	logVal := ctx.Value(loggerKey)
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
