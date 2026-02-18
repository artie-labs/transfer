package logger

import (
	"log/slog"
	"os"
	"runtime/debug"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	slogmulti "github.com/samber/slog-multi"
	slogsentry "github.com/samber/slog-sentry/v2"

	"github.com/artie-labs/transfer/lib/config"
)

var handlersToTerminate []func()

func NewLogger(verbose bool, sentryCfg *config.Sentry, version string) (*slog.Logger, func()) {
	tintLogLevel := slog.LevelInfo
	if verbose {
		tintLogLevel = slog.LevelDebug
	}

	handler := tint.NewHandler(os.Stderr, &tint.Options{
		Level:   tintLogLevel,
		NoColor: !isatty.IsTerminal(os.Stderr.Fd()),
	})
	if sentryCfg != nil && sentryCfg.DSN != "" {
		if err := sentry.Init(sentry.ClientOptions{
			Dsn:     sentryCfg.DSN,
			Release: "artie-transfer@" + version,
		}); err != nil {
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

// RecoverFatal is intended for use in defer. If the function panicked, it logs the panic and stack then exits.
// For goroutines that also need wg.Done(), use defer wg.Done() then defer RecoverFatal() so RecoverFatal runs first (LIFO).
func RecoverFatal() {
	if x := recover(); x != nil {
		Fatal("Recovered from panic", slog.Any("err", x), slog.String("stack", string(debug.Stack())))
	}
}

func Panic(msg string, args ...any) {
	slog.Error(msg, args...)
	runHandlers()
	panic(msg)
}
