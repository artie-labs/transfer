package converters

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"
)

func TestZonedTimestamp_Convert(t *testing.T) {
	{
		// Invalid data
		_, err := ZonedTimestamp{}.Convert(123)
		assert.ErrorContains(t, err, "expected string got '123' with type int")
	}
	{
		// Edge case (Year exceeds 9999)
		val, err := ZonedTimestamp{}.Convert("+275760-09-13T00:00:00.000000Z")
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
	{
		// Edge case (Negative year)
		val, err := ZonedTimestamp{}.Convert("-0999-10-10T10:10:10.000000Z")
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
	{
		// Valid
		{
			// No fractional seconds
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 000000000, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
		{
			// 1 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.1Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 100000000, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.0Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
		{
			// 2 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.12Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 120000000, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.00Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
		{
			// 3 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.123Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123000000, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.000Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
		{
			// 4 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.1234Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123400000, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.0000Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
		{
			// 5 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.12345Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123450000, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.00000Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
		{
			// 6 digits (microseconds)
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.123456Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123456000, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.000000Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
		{
			// 7 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.1234567Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123456700, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.0000000Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
		{
			// 8 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.12345678Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123456780, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.00000000Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
		{
			// 9 digits (nanoseconds)
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.123456789Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123456789, time.UTC), ext.TimestampTzKindType, "2006-01-02T15:04:05.000000000Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
		}
	}
}

func TestTime_Convert(t *testing.T) {
	{
		val, err := Time{}.Convert(int64(54720000))
		assert.NoError(t, err)

		extTime, isOk := val.(*ext.ExtendedTime)
		assert.True(t, isOk)
		assert.Equal(t, "15:12:00+00", extTime.String(""))
	}
}

func TestNanoTime_Converter(t *testing.T) {
	assert.Equal(t, typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType), NanoTime{}.ToKindDetails())
	{
		// Invalid data
		_, err := NanoTime{}.Convert("123")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid
		val, err := NanoTime{}.Convert(int64(54_720_000_009_000))
		assert.NoError(t, err)
		assert.Equal(t, "15:12:00.000009000", val.(*ext.ExtendedTime).String(""))
	}
}

func TestMicroTime_Converter(t *testing.T) {
	assert.Equal(t, typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType), MicroTime{}.ToKindDetails())
	{
		// Invalid data
		_, err := MicroTime{}.Convert("123")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid
		val, err := MicroTime{}.Convert(int64(54720000000))
		assert.NoError(t, err)
		assert.Equal(t, "15:12:00.000000", val.(*ext.ExtendedTime).String(""))
	}
}

func TestConvertTimeWithTimezone(t *testing.T) {
	{
		// Invalid
		ts, err := TimeWithTimezone{}.Convert("23:02")
		assert.Nil(t, ts)
		assert.ErrorContains(t, err, `failed to parse "23:02": parsing time`)
	}
	{
		// What Debezium + Reader would produce
		val, err := TimeWithTimezone{}.Convert("23:02:06.745116Z")
		ts, isOk := val.(*ext.ExtendedTime)
		assert.True(t, isOk)
		assert.NoError(t, err)

		expectedTs := ext.NewExtendedTime(time.Date(0, 1, 1, 23, 2, 6, 745116000, time.UTC), ext.TimeKindType, "15:04:05.000000Z")
		assert.Equal(t, expectedTs, ts)
	}
	{
		// Non UTC
		ts, err := TimeWithTimezone{}.Convert("23:02:06.745116")
		assert.ErrorContains(t, err, `failed to parse "23:02:06.745116"`)
		assert.Nil(t, ts)

		// Providing timezone offset
		ts, err = TimeWithTimezone{}.Convert("23:02:06.745116Z-07:00")
		assert.ErrorContains(t, err, `failed to parse "23:02:06.745116Z-07:00": parsing time "23:02:06.745116Z-07:00": extra text: "-07:00"`)
		assert.Nil(t, ts)
	}
}
