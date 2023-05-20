package bigquery

// https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time

const RFC3339Format = "%Y-%m-%dT%H:%M:%E*SZ"

// PostgresTimeFormatNoTZ does not contain TZ for BigQuery because BigQuery's `Time` type does not like time zones.
const PostgresTimeFormatNoTZ = "%H:%M:%E*S" // HH:MM:SS (micro-seconds)
