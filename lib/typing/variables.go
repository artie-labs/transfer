package typing

import "time"

const (
	PostgresTimeFormat     = PostgresTimeFormatNoTZ + TimezoneOffsetFormat
	PostgresTimeFormatNoTZ = "15:04:05.999999" // microsecond precision, used because certain destinations do not like `Time` types to specify tz locale
)

var supportedDateTimeLayouts = []string{
	// RFC 3339
	time.RFC3339,
	time.RFC3339Nano,
	RFC3339Millisecond,
	RFC3339Microsecond,
	RFC3339MicrosecondAlt,
	RFC3339Nanosecond,
	// Others
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.000-07:00",
	time.Layout,
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
	time.RFC1123,
	time.RFC1123Z,
}

var supportedDateFormats = []string{
	time.DateOnly,
}

var SupportedTimeFormats = []string{
	PostgresTimeFormat,
	PostgresTimeFormatNoTZ,
}

const TimezoneOffsetFormat = "Z07:00"

// RFC3339 variants
const (
	// Max precision up to microseconds (will trim away the trailing zeros)
	RFC3339MicroTZNoTZ = "2006-01-02T15:04:05.999999"
	RFC3339MicroTZ     = RFC3339MicroTZNoTZ + TimezoneOffsetFormat

	RFC3339NoTZ = "2006-01-02T15:04:05.999999999"

	RFC3339MillisecondNoTZ = "2006-01-02T15:04:05.000"
	RFC3339Millisecond     = RFC3339MillisecondNoTZ + TimezoneOffsetFormat

	RFC3339MicrosecondNoTZ = "2006-01-02T15:04:05.000000"
	RFC3339Microsecond     = RFC3339MicrosecondNoTZ + TimezoneOffsetFormat
	RFC3339MicrosecondAlt  = RFC3339MicrosecondNoTZ + "-07:00"

	RFC3339NanosecondNoTZ = "2006-01-02T15:04:05.000000000"
	RFC3339Nanosecond     = RFC3339NanosecondNoTZ + TimezoneOffsetFormat
)
