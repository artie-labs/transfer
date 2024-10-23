package converters

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
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
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:12Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 12, 000000000, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:12Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
		{
			// 1 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.1Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 100000000, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:00.1Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
		{
			// 2 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.12Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 120000000, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:00.12Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
		{
			// 3 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.123Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123000000, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:00.123Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
		{
			// 4 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.1234Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123400000, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:00.1234Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
		{
			// 5 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.12345Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123450000, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:00.12345Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
		{
			// 6 digits (microseconds)
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.123456Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123456000, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:00.123456Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
		{
			// 7 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.1234567Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123456700, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:00.1234567Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
		{
			// 8 digits
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.12345678Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123456780, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:00.12345678Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
		{
			// 9 digits (nanoseconds)
			val, err := ZonedTimestamp{}.Convert("2025-09-13T00:00:00.123456789Z")
			assert.NoError(t, err)

			expectedExtTime := ext.NewExtendedTime(time.Date(2025, time.September, 13, 0, 0, 0, 123456789, time.UTC), ext.TimestampTZKindType, "2006-01-02T15:04:05.999999999Z")
			assert.Equal(t, expectedExtTime, val.(*ext.ExtendedTime))
			assert.Equal(t, "2025-09-13T00:00:00.123456789Z", val.(*ext.ExtendedTime).GetTime().Format(ZonedTimestamp{}.layout()))
		}
	}
}

func TestTime_Convert(t *testing.T) {
	{
		val, err := Time{}.Convert(int64(54720321))
		assert.NoError(t, err)

		extTime, isOk := val.(*ext.ExtendedTime)
		assert.True(t, isOk)
		assert.Equal(t, "15:12:00.321", extTime.GetTime().Format(Time{}.layout()))
	}
	{
		val, err := Time{}.Convert(int64(54720000))
		assert.NoError(t, err)

		extTime, isOk := val.(*ext.ExtendedTime)
		assert.True(t, isOk)
		assert.Equal(t, "15:12:00.000", extTime.GetTime().Format(Time{}.layout()))
	}
}

func TestNanoTime_Converter(t *testing.T) {
	assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, NanoTime{}.layout()), NanoTime{}.ToKindDetails())
	{
		// Invalid data
		_, err := NanoTime{}.Convert("123")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid
		val, err := NanoTime{}.Convert(int64(54_720_000_009_000))
		assert.NoError(t, err)
		assert.Equal(t, "15:12:00.000009000", val.(*ext.ExtendedTime).GetTime().Format(NanoTime{}.layout()))
	}
}

func TestMicroTime_Converter(t *testing.T) {
	assert.Equal(t, typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, MicroTime{}.layout()), MicroTime{}.ToKindDetails())
	{
		// Invalid data
		_, err := MicroTime{}.Convert("123")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		// Valid
		val, err := MicroTime{}.Convert(int64(54720000000))
		assert.NoError(t, err)
		assert.Equal(t, "15:12:00.000000", val.(*ext.ExtendedTime).GetTime().Format(MicroTime{}.layout()))
	}
}

func TestConvertTimeWithTimezone(t *testing.T) {
	{
		// Invalid
		{
			// Malformed
			_, err := TimeWithTimezone{}.Convert("23:02")
			assert.ErrorContains(t, err, `failed to parse "23:02": parsing time`)
		}
		{
			// Non UTC
			_, err := TimeWithTimezone{}.Convert("23:02:06.745116")
			assert.ErrorContains(t, err, `failed to parse "23:02:06.745116"`)
		}
		{
			// Providing timezone offset
			_, err := TimeWithTimezone{}.Convert("23:02:06.745116Z-07:00")
			assert.ErrorContains(t, err, `failed to parse "23:02:06.745116Z-07:00": parsing time "23:02:06.745116Z-07:00": extra text: "-07:00"`)
		}
	}
	{
		// What Debezium + Reader would produce (microsecond precision)
		val, err := TimeWithTimezone{}.Convert("23:02:06.745116Z")
		assert.NoError(t, err)

		expectedTs := ext.NewExtendedTime(time.Date(0, 1, 1, 23, 2, 6, 745116000, time.UTC), ext.TimeKindType, TimeWithTimezone{}.layout())
		assert.Equal(t, expectedTs, val.(*ext.ExtendedTime))
		assert.Equal(t, "23:02:06.745116Z", val.(*ext.ExtendedTime).GetTime().Format(TimeWithTimezone{}.layout()))
	}
	{
		// ms precision
		val, err := TimeWithTimezone{}.Convert("23:02:06.745Z")
		assert.NoError(t, err)

		expectedTs := ext.NewExtendedTime(time.Date(0, 1, 1, 23, 2, 6, 745000000, time.UTC), ext.TimeKindType, TimeWithTimezone{}.layout())
		assert.Equal(t, expectedTs, val.(*ext.ExtendedTime))
		assert.Equal(t, "23:02:06.745Z", val.(*ext.ExtendedTime).GetTime().Format(TimeWithTimezone{}.layout()))
	}
	{
		// no fractional seconds
		val, err := TimeWithTimezone{}.Convert("23:02:06Z")
		assert.NoError(t, err)

		expectedTs := ext.NewExtendedTime(time.Date(0, 1, 1, 23, 2, 6, 0, time.UTC), ext.TimeKindType, TimeWithTimezone{}.layout())
		assert.Equal(t, expectedTs, val.(*ext.ExtendedTime))
		assert.Equal(t, "23:02:06Z", val.(*ext.ExtendedTime).GetTime().Format(TimeWithTimezone{}.layout()))
	}
}
