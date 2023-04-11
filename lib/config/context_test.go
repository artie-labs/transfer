package config

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseArgs(t *testing.T) {
	ctx := InitializeCfgIntoContext(context.Background(), []string{}, false)
	settings := FromContext(ctx)

	assert.Equal(t, settings.VerboseLogging, false)
	assert.Nil(t, settings.Config)

	ctx = InitializeCfgIntoContext(context.Background(), []string{"-v"}, false)
	settings = FromContext(ctx)
	assert.Equal(t, settings.VerboseLogging, true)
}
