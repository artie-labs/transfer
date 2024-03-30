package values

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"
)

func TestBooleanToBit(t *testing.T) {
	assert.Equal(t, 1, BooleanToBit(true))
	assert.Equal(t, 0, BooleanToBit(false))
}

func TestToString(t *testing.T) {
	{
		// Nil value
		_, err := ToString(nil, columns.Column{}, nil)
		assert.ErrorContains(t, err, "colVal is nil")
	}
	{
		// ETime
		eTimeCol := columns.NewColumn("time", typing.ETime)
		_, err := ToString("2021-01-01T00:00:00Z", eTimeCol, nil)
		assert.ErrorContains(t, err, "column kind details for extended time details is null")

		eTimeCol.KindDetails.ExtendedTimeDetails = &ext.NestedKind{Type: ext.TimeKindType}
		// Using `string`
		val, err := ToString("2021-01-01T03:52:00Z", eTimeCol, nil)
		assert.Equal(t, "03:52:00", val)

		// Using `*ExtendedTime`
		dustyBirthday := time.Date(2019, time.December, 31, 0, 0, 0, 0, time.UTC)
		originalFmt := "2006-01-02T15:04:05Z07:00"
		extendedTime, err := ext.NewExtendedTime(dustyBirthday, ext.DateTimeKindType, originalFmt)
		assert.NoError(t, err)

		eTimeCol.KindDetails.ExtendedTimeDetails = &ext.NestedKind{Type: ext.DateTimeKindType}
		actualValue, err := ToString(extendedTime, eTimeCol, nil)
		assert.NoError(t, err)
		assert.Equal(t, extendedTime.String(originalFmt), actualValue)
	}
	{
		// String

		// 1. JSON
		val, err := ToString(map[string]any{"foo": "bar"}, columns.Column{KindDetails: typing.String}, nil)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)

		// 2. Array
		val, err = ToString([]string{"foo", "bar"}, columns.Column{KindDetails: typing.String}, nil)
		assert.NoError(t, err)
		assert.Equal(t, `["foo","bar"]`, val)

		// 3. Normal strings
		val, err = ToString("foo", columns.Column{KindDetails: typing.String}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "foo", val)
	}
	{

	}
}
