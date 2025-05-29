package typing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsupportedDataTypeError(t *testing.T) {
	assert.True(t, IsUnsupportedDataTypeError(NewUnsupportedDataTypeError("foo")))

	// Not relevant
	assert.False(t, IsUnsupportedDataTypeError(fmt.Errorf("foo")))

	// Nil
	assert.False(t, IsUnsupportedDataTypeError(nil))
}
