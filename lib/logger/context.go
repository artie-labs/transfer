package logger

import (
	"context"
	"github.com/sirupsen/logrus"
)

const (
	loggerKey = "_log"
)

func InjectLoggerIntoCtx(logger *logrus.Logger, ctx context.Context) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

func FromContext(ctx context.Context) *logrus.Logger {
	logVal := ctx.Value(loggerKey)
	if logVal == nil {
		// Inject this back into context, so we don't need to initialize this again
		return FromContext(InjectLoggerIntoCtx(NewLogger(), ctx))
	}

	log, isOk := logVal.(*logrus.Logger)
	if !isOk {
		return FromContext(InjectLoggerIntoCtx(NewLogger(), ctx))
	}

	return log
}
