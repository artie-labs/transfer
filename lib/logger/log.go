package logger

import (
	"os"

	"github.com/evalphobia/logrus_sentry"
	"github.com/sirupsen/logrus"

	"github.com/artie-labs/transfer/lib/config"
)

func NewLogger(settings *config.Settings) *logrus.Logger {
	log := logrus.New()
	log.SetOutput(os.Stdout)

	if settings != nil && settings.Config != nil && settings.Config.Reporting.Sentry != nil && settings.Config.Reporting.Sentry.DSN != "" {
		hook, err := logrus_sentry.NewSentryHook(settings.Config.Reporting.Sentry.DSN, []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
		})

		if err != nil {
			log.WithError(err).Warn("Failed to enable Sentry output")
		} else {
			log.Hooks.Add(hook)
		}
	}

	return log
}
