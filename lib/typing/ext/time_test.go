package ext

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExtTimeIsValid(t *testing.T) {
	{
		ext := ExtendedTime{
			Time: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		assert.True(t, ext.IsValid())
	}
	{
		ext := ExtendedTime{
			Time: time.Date(20020, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		assert.False(t, ext.IsValid())
	}
	{
		ext := ExtendedTime{
			Time: time.Date(-1300, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		assert.False(t, ext.IsValid())
	}
}
