package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseArgs(t *testing.T) {
	ParseArgs([]string{}, false)
	settings := GetSettings()

	assert.Equal(t, settings.VerboseLogging, false)
	assert.Nil(t, settings.Config)

	ParseArgs([]string{"-v"}, false)
	assert.Equal(t, GetSettings().VerboseLogging, true)
}
