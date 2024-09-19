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
	RFC3339MillisecondUTC,
	time.RFC3339Nano,
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

// RFC3339 variants
const (
	RFC3339MillisecondUTC = "2006-01-02T15:04:05.000Z"
	RFC3339Millisecond    = "2006-01-02T15:04:05.000Z07:00"
	RFC3339Microsecond    = "2006-01-02T15:04:05.000000Z07:00"
	RFC3339Nanosecond     = "2006-01-02T15:04:05.000000000Z07:00"
)
