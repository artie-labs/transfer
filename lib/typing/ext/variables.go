package ext

import "time"

const (
	ISO8601                = "2006-01-02T15:04:05.999-07:00"
	PostgresDateFormat     = "2006-01-02"
	PostgresTimeFormat     = "15:04:05.999999-07" // microsecond precision
	AdditionalTimeFormat   = "15:04:05.999999Z07"
	PostgresTimeFormatNoTZ = "15:04:05.999999" // microsecond precision, used because certain destinations do not like `Time` types to specify tz locale
)

var supportedDateTimeLayouts = []string{
	// UTC
	RFC3339MillisecondUTC,
	RFC3339MicrosecondUTC,
	RFC3339NanosecondUTC,
	// RFC 3339
	RFC3339Millisecond,
	RFC3339Microsecond,
	RFC3339Nanosecond,
	time.RFC3339Nano,
	// Others
	ISO8601,
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
	AdditionalTimeFormat,
}

const TimezoneOffsetFormat = "Z07:00"

// RFC3339 variants
const (
	RFC3339MillisecondUTC  = "2006-01-02T15:04:05.000Z"
	RFC3339MicrosecondUTC  = "2006-01-02T15:04:05.000000Z"
	RFC3339NanosecondUTC   = "2006-01-02T15:04:05.000000000Z"
	RFC3339Millisecond     = "2006-01-02T15:04:05.000" + TimezoneOffsetFormat
	RFC3339MillisecondNoTZ = "2006-01-02T15:04:05.000"
	RFC3339Microsecond     = "2006-01-02T15:04:05.000000" + TimezoneOffsetFormat
	RFC3339MicrosecondNoTZ = "2006-01-02T15:04:05.000000"
	RFC3339Nanosecond      = "2006-01-02T15:04:05.000000000" + TimezoneOffsetFormat
	RFC3339NanosecondNoTZ  = "2006-01-02T15:04:05.000000000"
)
