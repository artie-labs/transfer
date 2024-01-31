package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseArgs(t *testing.T) {
	ctx, err := InitializeCfgIntoContext(context.Background(), []string{}, false)
	assert.NoError(t, err)
	settings := FromContext(ctx)

	assert.Equal(t, settings.VerboseLogging, false)

	ctx, err = InitializeCfgIntoContext(context.Background(), []string{"-v"}, false)
	assert.NoError(t, err)
	settings = FromContext(ctx)
	assert.Equal(t, settings.VerboseLogging, true)
}
