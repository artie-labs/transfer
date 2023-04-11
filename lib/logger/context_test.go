package logger

import (
	"context"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogger(t *testing.T) {
	ctx := config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: true,
	})

	log := &logrus.Logger{
		Level: logrus.DebugLevel,
	}

	assert.Equal(t, log.Level, FromContext(InjectLoggerIntoCtx(ctx)).Level)
}
