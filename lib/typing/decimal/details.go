package decimal

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/ptr"
)

type DecimalDetails struct {
	scale     int
	precision *int
}

func NewDecimalDetails(precision *int, scale int) *DecimalDetails {
	if precision != nil {
		if scale > *precision && *precision != -1 {
			// Note: -1 precision means it's not specified.

			// This is typically not possible, but Postgres has a design flaw that allows you to do things like: NUMERIC(5, 6) which actually equates to NUMERIC(7, 6)
			// We are setting precision to be scale + 1 to account for the leading zero for decimal numbers.
			precision = ptr.ToInt(scale + 1)
		}
	}

	return &DecimalDetails{
		scale:     scale,
		precision: precision,
	}
}

func (d DecimalDetails) Scale() int {
	return d.scale
}

func (d DecimalDetails) Precision() *int {
	return d.precision
}

// SnowflakeKind - is used to determine whether a NUMERIC data type should be a STRING or NUMERIC(p, s).
func (d *DecimalDetails) SnowflakeKind() string {
	return d.toKind(MaxPrecisionBeforeString, "STRING")
}

// MsSQLKind - Has the same limitation as Redshift
// Spec: https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-ver16#arguments
func (d *DecimalDetails) MsSQLKind() string {
	return d.toKind(MaxPrecisionBeforeString, "TEXT")
}

// RedshiftKind - is used to determine whether a NUMERIC data type should be a TEXT or NUMERIC(p, s).
func (d *DecimalDetails) RedshiftKind() string {
	return d.toKind(MaxPrecisionBeforeString, "TEXT")
}

// BigQueryKind - is inferring logic from: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
func (d *DecimalDetails) BigQueryKind() string {
	if d.isNumeric() {
		return fmt.Sprintf("NUMERIC(%v, %v)", *d.precision, d.scale)
	} else if d.isBigNumeric() {
		return fmt.Sprintf("BIGNUMERIC(%v, %v)", *d.precision, d.scale)
	}

	return "STRING"
}
