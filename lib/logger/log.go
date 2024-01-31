package logger

import (
	"log/slog"
	"os"

	"github.com/getsentry/sentry-go"
	"github.com/lmittmann/tint"
	slogmulti "github.com/samber/slog-multi"
	slogsentry "github.com/samber/slog-sentry"

	"github.com/artie-labs/transfer/lib/config"
)

func NewLogger(verbose bool, sentryCfg *config.Sentry) (*slog.Logger, bool) {
	tintLogLevel := slog.LevelInfo
	if verbose {
		tintLogLevel = slog.LevelDebug
	}

	handler := tint.NewHandler(os.Stderr, &tint.Options{Level: tintLogLevel})

	var loggingToSentry bool
	if sentryCfg != nil && sentryCfg.DSN != "" {
		if err := sentry.Init(sentry.ClientOptions{Dsn: sentryCfg.DSN}); err != nil {
			slog.New(handler).Warn("Failed to enable Sentry output", slog.Any("err", err))
		} else {
			handler = slogmulti.Fanout(
				handler,
				slogsentry.Option{Level: slog.LevelError}.NewSentryHandler(),
			)
			loggingToSentry = true
		}
	}

	return slog.New(handler), loggingToSentry
}

func Fatal(msg string, args ...interface{}) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func Panic(msg string, args ...interface{}) {
	slog.Error(msg, args...)
	panic(msg)
}
