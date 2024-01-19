package logger

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/getsentry/sentry-go"
	"github.com/lmittmann/tint"
	slogmulti "github.com/samber/slog-multi"
	slogsentry "github.com/samber/slog-sentry"

	"github.com/artie-labs/transfer/lib/config"
)

func NewLogger(settings *config.Settings) *slog.Logger {
	tintLogLevel := slog.LevelInfo
	if settings != nil && settings.VerboseLogging {
		tintLogLevel = slog.LevelDebug
	}

	handler := tint.NewHandler(os.Stderr, &tint.Options{Level: tintLogLevel})

	var loggingToSentry bool
	if settings != nil && settings.Config != nil && settings.Config.Reporting.Sentry != nil && settings.Config.Reporting.Sentry.DSN != "" {
		if err := sentry.Init(sentry.ClientOptions{Dsn: settings.Config.Reporting.Sentry.DSN}); err != nil {
			slog.New(handler).Warn("Failed to enable Sentry output", slog.Any("err", err))
		} else {
			handler = slogmulti.Fanout(
				handler,
				slogsentry.Option{Level: slog.LevelWarn}.NewSentryHandler(),
			)
			loggingToSentry = true
		}
	}

	logger := slog.New(handler)
	if loggingToSentry {
		logger.Info("sentry logger enabled")
	}
	return logger
}

func Fatal(msg string, args ...interface{}) {
	slog.Error(msg, args...)
	panic(msg)
}

func Fatalf(msg string, args ...any) {
	msg = fmt.Sprintf(msg, args...)
	slog.Error(msg)
	panic(msg)
}
