package typing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultValueFromPtr(t *testing.T) {
	{
		// ptr is not set
		assert.Equal(t, int32(5), DefaultValueFromPtr[int32](nil, int32(5)))
	}
	{
		// ptr is set
		assert.Equal(t, int32(10), DefaultValueFromPtr[int32](ToPtr(int32(10)), int32(5)))
	}
}
