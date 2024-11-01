package ext

import "time"

const (
	PostgresDateFormat     = "2006-01-02"
	PostgresTimeFormat     = PostgresTimeFormatNoTZ + TimezoneOffsetFormat
	PostgresTimeFormatNoTZ = "15:04:05.999999" // microsecond precision, used because certain destinations do not like `Time` types to specify tz locale
)

var supportedDateTimeLayouts = []string{
	// RFC 3339
	time.RFC3339Nano,
	RFC3339Millisecond,
	RFC3339Microsecond,
	RFC3339Nanosecond,
	time.RFC3339Nano,
	// Others
	time.Layout,
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
	time.RFC1123,
	time.RFC1123Z,
	time.RFC3339,
}

var supportedDateFormats = []string{
	PostgresDateFormat,
}

var SupportedTimeFormats = []string{
	PostgresTimeFormat,
	PostgresTimeFormatNoTZ,
}

const TimezoneOffsetFormat = "Z07:00"

// RFC3339 variants
const (
	RFC3339NoTZ            = "2006-01-02T15:04:05.999999999"
	RFC3339Millisecond     = "2006-01-02T15:04:05.000" + TimezoneOffsetFormat
	RFC3339MillisecondNoTZ = "2006-01-02T15:04:05.000"
	RFC3339Microsecond     = "2006-01-02T15:04:05.000000" + TimezoneOffsetFormat
	RFC3339MicrosecondNoTZ = "2006-01-02T15:04:05.000000"
	RFC3339Nanosecond      = "2006-01-02T15:04:05.000000000" + TimezoneOffsetFormat
	RFC3339NanosecondNoTZ  = "2006-01-02T15:04:05.000000000"
)
