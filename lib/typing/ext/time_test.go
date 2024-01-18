package ext

import "time"

func (e *ExtTestSuite) TestString() {
	{
		// Test DateTime
		extendedTime, err := NewExtendedTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), DateTimeKindType, "")
		e.NoError(err)
		e.Equal("2020-01-01T00:00:00Z", extendedTime.String(""))
	}
	{
		// Test year 10k, should be empty string.
		extendedTime, err := NewExtendedTime(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), DateTimeKindType, "")
		e.NoError(err)
		e.Equal("", extendedTime.String(""))
	}

	{
		// Test year in BC, should also be empty string
		extendedTime, err := NewExtendedTime(time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), DateTimeKindType, "")
		e.NoError(err)
		e.Equal("", extendedTime.String(""))
	}
}
