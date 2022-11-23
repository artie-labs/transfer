package logger

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogger(t *testing.T) {
	ctx := context.Background()
	log := &logrus.Logger{
		Level: logrus.DebugLevel,
	}

	assert.Equal(t, log.Level, FromContext(InjectLoggerIntoCtx(log, ctx)).Level)
}

func TestLoggerNil(t *testing.T) {
	assert.NotNil(t, FromContext(context.Background()))
}

func TestLoggerWrongType(t *testing.T) {
	ctx := context.WithValue(context.Background(), loggerKey, "foo")
	assert.NotNil(t, FromContext(ctx))
}

func TestLoggerSubsequent(t *testing.T) {
	// Start with nil
	// Get the logger, then update the level
	// Fetch the logger again and ensure level is the same.
	ctx := context.Background()
	log := FromContext(ctx)
	assert.NotNil(t, log)

	log.Level = logrus.DebugLevel
	ctx = InjectLoggerIntoCtx(log, ctx)

	assert.Equal(t, FromContext(ctx).Level, log.Level)
}
