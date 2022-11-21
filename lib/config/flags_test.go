package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseArgs(t *testing.T) {
	assert.Panics(t, func() { ParseArgs([]string{}) }, "no args")
}
