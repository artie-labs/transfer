package typing

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"
)

func TestKindDetails_ToArrowType(t *testing.T) {
	{
		// String field
		for _, kd := range []KindDetails{String, Struct} {
			arrowType, err := kd.ToArrowType(nil)
			assert.NoError(t, err)
			assert.Equal(t, arrow.BinaryTypes.String, arrowType)
		}
	}
	{
		// Integers
		arrowType, err := Integer.ToArrowType(nil)
		assert.NoError(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Int64, arrowType)
	}
	{
		// Float
		arrowType, err := Float.ToArrowType(nil)
		assert.NoError(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Float64, arrowType)
	}
	{
		// Boolean
		arrowType, err := Boolean.ToArrowType(nil)
		assert.NoError(t, err)
		assert.Equal(t, arrow.FixedWidthTypes.Boolean, arrowType)
	}
	{
		// Time
		arrowType, err := Time.ToArrowType(nil)
		assert.NoError(t, err)
		assert.Equal(t, arrow.FixedWidthTypes.Time32ms, arrowType)
	}
	{
		// Date
		arrowType, err := Date.ToArrowType(nil)
		assert.NoError(t, err)
		assert.Equal(t, arrow.FixedWidthTypes.Date32, arrowType)
	}
	{
		// Array
		arrowType, err := Array.ToArrowType(nil)
		assert.NoError(t, err)
		assert.Equal(t, arrow.ListOf(arrow.BinaryTypes.String), arrowType)
	}
	{
		// TimestampTZ with location
		loc, err := time.LoadLocation("America/New_York")
		assert.NoError(t, err)

		arrowType, err := TimestampTZ.ToArrowType(loc)
		assert.NoError(t, err)

		timestampType, ok := arrowType.(*arrow.TimestampType)
		assert.True(t, ok)
		assert.Equal(t, arrow.Millisecond, timestampType.Unit)
		assert.Equal(t, "", timestampType.TimeZone) // Should be timezone-naive when location is provided
	}
	{
		// TimestampTZ without location
		arrowType, err := TimestampTZ.ToArrowType(nil)
		assert.NoError(t, err)
		assert.Equal(t, arrow.FixedWidthTypes.Timestamp_ms, arrowType)
	}
}

func TestKindDetails_ParseValueForArrow(t *testing.T) {
	{
		// String
		value, err := String.ParseValueForArrow("test", nil)
		assert.NoError(t, err)
		assert.Equal(t, "test", value)
	}
	{
		// Integer
		value, err := Integer.ParseValueForArrow(int64(123), nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(123), value)
	}
	{
		// Boolean
		value, err := Boolean.ParseValueForArrow(true, nil)
		assert.NoError(t, err)
		assert.Equal(t, true, value)
	}
	{
		// Float
		value, err := Float.ParseValueForArrow(float32(1.23), nil)
		assert.NoError(t, err)
		assert.Equal(t, float32(1.23), value)
	}
	{
		// Time
		testTime := time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC)
		value, err := Time.ParseValueForArrow(testTime, nil)
		assert.NoError(t, err)

		// Should be milliseconds since midnight
		expectedMillis := int32((15*60*60 + 30*60 + 45) * 1000)
		assert.Equal(t, expectedMillis, value)
	}
	{
		// Date
		testDate := time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC)
		value, err := Date.ParseValueForArrow(testDate, nil)
		assert.NoError(t, err)

		// Should be days since epoch (1970-01-01)
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		expectedDays := int32(testDate.Sub(epoch).Hours() / 24)
		assert.Equal(t, expectedDays, value)
	}
	{
		// Nil value
		value, err := String.ParseValueForArrow(nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
}
