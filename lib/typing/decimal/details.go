package decimal

import (
	"fmt"
	"log/slog"
)

const (
	DefaultScale int32 = 5
	// MaxPrecisionBeforeString - if the precision is greater than 38, we'll cast it as a string.
	// This is because Snowflake and BigQuery both do not have NUMERIC data types that go beyond 38.
	MaxPrecisionBeforeString int32 = 38
)

type Details struct {
	scale     int32
	precision int32
}

// TwosComplementByteArrLength - returns the length of the twos complement byte array for the decimal.
// This is used to determine the length of the byte array for the decimal.
func (d Details) TwosComplementByteArrLength() int32 {
	return (d.precision + 1) / 2
}

func (d Details) NotSet() bool {
	return d.precision == PrecisionNotSpecified && d.scale == DefaultScale
}

func NewDetails(precision, scale int32) Details {
	if precision == 0 {
		// MySQL, PostgreSQL, and SQLServer do not allow a zero precision, so this should never happen.
		// Let's log if we observe it happening, and if we don't see it in the logs then we can use zero as the
		// [PrecisionNotSpecified] value and change precision to a uint16.
		slog.Error("Decimal precision is zero")
	}

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

func (d Details) IcebergKind() string {
	return d.toDecimalKind(MaxPrecisionBeforeString, "STRING")
}

// SnowflakeKind - is used to determine whether a NUMERIC data type should be a STRING or NUMERIC(p, s).
func (d Details) SnowflakeKind() string {
	return d.toKind(MaxPrecisionBeforeString, "STRING")
}

// DatabricksKind - is used to determine whether a NUMERIC data type should be a STRING or NUMERIC(p, s).
// Ref: https://docs.databricks.com/en/sql/language-manual/data-types/decimal-type.html
func (d Details) DatabricksKind() string {
	return d.toDecimalKind(MaxPrecisionBeforeString, "STRING")
}

// MsSQLKind - Has the same limitation as Redshift
// Spec: https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-ver16#arguments
func (d Details) MsSQLKind() string {
	return d.toKind(MaxPrecisionBeforeString, "VARCHAR(MAX)")
}

// RedshiftKind - is used to determine whether a NUMERIC data type should be a TEXT or NUMERIC(p, s).
func (d Details) RedshiftKind() string {
	return d.toKind(MaxPrecisionBeforeString, "TEXT")
}

// BigQueryKind - is inferring logic from: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
func (d Details) BigQueryKind(useBigNumericForVariableNumeric bool) string {
	if d.precision == PrecisionNotSpecified {
		if useBigNumericForVariableNumeric {
			return "BIGNUMERIC"
		} else {
			return "NUMERIC"
		}
	}

	if d.isNumeric() {
		return fmt.Sprintf("NUMERIC(%v, %v)", d.precision, d.scale)
	} else if d.isBigNumeric() {
		return fmt.Sprintf("BIGNUMERIC(%v, %v)", d.precision, d.scale)
	}

	return "STRING"
}

func (d Details) PostgresKind() string {
	if d.precision == PrecisionNotSpecified {
		return "NUMERIC"
	}

	return fmt.Sprintf("NUMERIC(%d, %d)", d.precision, d.scale)
}

func (d Details) DuckDBKind() string {
	if d.precision == PrecisionNotSpecified {
		return "DECIMAL"
	}

	if d.precision > MaxPrecisionBeforeString {
		return "TEXT"
	}

	return fmt.Sprintf("DECIMAL(%d, %d)", d.precision, d.scale)
}
