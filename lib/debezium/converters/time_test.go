package converters

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"
)

func TestConvertDateTimeWithTimezone(t *testing.T) {
	{
		// Invalid data
		_, err := ConvertDateTimeWithTimezone(123)
		assert.ErrorContains(t, err, "expected string got '123' with type int")
	}
	{
		// Edge case (Year exceeds 9999)
		val, err := ConvertDateTimeWithTimezone("+275760-09-13T00:00:00.000000Z")
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
	{
		// Edge case (Negative year)
		val, err := ConvertDateTimeWithTimezone("-0999-10-10T10:10:10.000000Z")
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
	{
		// Valid
		val, err := ConvertDateTimeWithTimezone("2025-09-13T00:00:00.000000Z")
		assert.NoError(t, err)

		expectedExtTime := &ext.ExtendedTime{
			Time: time.Date(2025, time.September, 13, 0, 0, 0, 0, time.UTC),
			NestedKind: ext.NestedKind{
				Type:   ext.DateTimeKindType,
				Format: "2006-01-02T15:04:05Z07:00",
			},
		}

		assert.Equal(t, expectedExtTime, val)
	}
}

func TestConvertTimeWithTimezone(t *testing.T) {
	{
		// Invalid
		ts, err := ConvertTimeWithTimezone("23:02")
		assert.Nil(t, ts)
		assert.ErrorContains(t, err, `failed to parse "23:02": parsing time`)
	}
	{
		// What Debezium + Reader would produce
		ts, err := ConvertTimeWithTimezone("23:02:06.745116Z")
		assert.NoError(t, err)
		expectedTs := &ext.ExtendedTime{
			Time: time.Date(0, 1, 1, 23, 2, 6, 745116000, time.UTC),
			NestedKind: ext.NestedKind{
				Type:   ext.TimeKindType,
				Format: "15:04:05.000000Z",
			},
		}

		assert.Equal(t, expectedTs, ts)
	}
}
