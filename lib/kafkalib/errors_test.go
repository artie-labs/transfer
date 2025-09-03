package kafkalib

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFetchMessageError(t *testing.T) {
	{
		// Valid:
		err := NewFetchMessageError(fmt.Errorf("test error"))
		assert.Equal(t, `failed to fetch message: "test error"`, err.Error())
		assert.True(t, IsFetchMessageError(err))
	}
	{
		// Invalid:
		for _, variant := range []error{fmt.Errorf("test"), nil} {
			assert.False(t, IsFetchMessageError(variant))
		}
	}
}
