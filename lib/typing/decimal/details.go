package decimal

import (
	"fmt"
)

type Details struct {
	scale     int32
	precision int32
}

func NewDetails(precision int32, scale int32) Details {
	if scale > precision && precision != PrecisionNotSpecified {
		// Note: -1 precision means it's not specified.

		// This is typically not possible, but Postgres has a design flaw that allows you to do things like: NUMERIC(5, 6) which actually equates to NUMERIC(7, 6)
		// We are setting precision to be scale + 1 to account for the leading zero for decimal numbers.
		precision = scale + 1
	}

	return Details{
		scale:     scale,
		precision: precision,
	}
}

func (d Details) Scale() int32 {
	return d.scale
}

func (d Details) Precision() int32 {
	return d.precision
}

// SnowflakeKind - is used to determine whether a NUMERIC data type should be a STRING or NUMERIC(p, s).
func (d Details) SnowflakeKind() string {
	return d.toKind(MaxPrecisionBeforeString, "STRING")
}

// MsSQLKind - Has the same limitation as Redshift
// Spec: https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-ver16#arguments
func (d Details) MsSQLKind() string {
	return d.toKind(MaxPrecisionBeforeString, "TEXT")
}

// RedshiftKind - is used to determine whether a NUMERIC data type should be a TEXT or NUMERIC(p, s).
func (d Details) RedshiftKind() string {
	return d.toKind(MaxPrecisionBeforeString, "TEXT")
}

// BigQueryKind - is inferring logic from: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
func (d Details) BigQueryKind() string {
	if d.isNumeric() {
		return fmt.Sprintf("NUMERIC(%v, %v)", d.precision, d.scale)
	} else if d.isBigNumeric() {
		return fmt.Sprintf("BIGNUMERIC(%v, %v)", d.precision, d.scale)
	}

	return "STRING"
}
