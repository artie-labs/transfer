package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseArgs(t *testing.T) {
	settings, err := LoadSettings([]string{}, false)
	assert.NoError(t, err)
	assert.Equal(t, settings.VerboseLogging, false)

	settings, err = LoadSettings([]string{"-v"}, false)
	assert.NoError(t, err)
	assert.Equal(t, settings.VerboseLogging, true)
}
