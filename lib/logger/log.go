package logger

import (
	"log/slog"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/lmittmann/tint"
	slogmulti "github.com/samber/slog-multi"
	slogsentry "github.com/samber/slog-sentry/v2"

	"github.com/artie-labs/transfer/lib/config"
)

var handlersToTerminate []func()

func NewLogger(verbose bool, sentryCfg *config.Sentry) (*slog.Logger, func()) {
	tintLogLevel := slog.LevelInfo
	if verbose {
		tintLogLevel = slog.LevelDebug
	}

	handler := tint.NewHandler(os.Stderr, &tint.Options{Level: tintLogLevel})
	if sentryCfg != nil && sentryCfg.DSN != "" {
		if err := sentry.Init(sentry.ClientOptions{Dsn: sentryCfg.DSN}); err != nil {
			slog.New(handler).Warn("Failed to enable Sentry output", slog.Any("err", err))
		} else {
			slog.New(handler).Info("Sentry logger enabled")
			handler = slogmulti.Fanout(handler, slogsentry.Option{Level: slog.LevelError}.NewSentryHandler())
			handlersToTerminate = append(handlersToTerminate, func() {
				sentry.Flush(2 * time.Second)
			})
		}
	}

	return slog.New(handler), runHandlers
}

func runHandlers() {
	for _, handlerToTerminate := range handlersToTerminate {
		handlerToTerminate()
	}
}

func Fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	runHandlers()
	os.Exit(1)
}

func Panic(msg string, args ...any) {
	slog.Error(msg, args...)
	runHandlers()
	panic(msg)
}
