package ext

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExtendedTime_MarshalJSON(t *testing.T) {
	extTime := NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123456000, time.UTC), DateTimeKindType, RFC3339Millisecond)

	{
		// Single value
		bytes, err := json.Marshal(extTime)
		assert.NoError(t, err)
		assert.Equal(t, `"2025-09-13T00:00:00.123Z"`, string(bytes))
	}
	{
		// As a nested object
		type Object struct {
			ExtendedTime *ExtendedTime `json:"extendedTime"`
			Foo          string        `json:"foo"`
		}

		var obj Object
		obj.ExtendedTime = extTime
		obj.Foo = "bar"

		bytes, err := json.Marshal(obj)
		assert.NoError(t, err)
		assert.Equal(t, `{"extendedTime":"2025-09-13T00:00:00.123Z","foo":"bar"}`, string(bytes))
	}
}
