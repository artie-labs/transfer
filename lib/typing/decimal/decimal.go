package decimal

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/cockroachdb/apd/v3"
)

// Decimal is Artie's wrapper around [apd.Decimal] which can store large numbers w/ no precision loss.
type Decimal struct {
	scale     int
	precision *int
	value     *apd.Decimal
}

const (
	DefaultScale          = 5
	PrecisionNotSpecified = -1
	// MaxPrecisionBeforeString - if the precision is greater than 38, we'll cast it as a string.
	// This is because Snowflake and BigQuery both do not have NUMERIC data types that go beyond 38.
	MaxPrecisionBeforeString = 38
)

func NewDecimal(precision *int, scale int, value *apd.Decimal) *Decimal {
	if precision != nil {
		if scale > *precision && *precision != -1 {
			// Note: -1 precision means it's not specified.

			// This is typically not possible, but Postgres has a design flaw that allows you to do things like: NUMERIC(5, 6) which actually equates to NUMERIC(7, 6)
			// We are setting precision to be scale + 1 to account for the leading zero for decimal numbers.
			precision = ptr.ToInt(scale + 1)
		}
	}

	return &Decimal{
		scale:     scale,
		precision: precision,
		value:     value,
	}
}

func (d *Decimal) Scale() int {
	return d.scale
}

func (d *Decimal) Precision() *int {
	return d.precision
}

// String() is used to override fmt.Sprint(val), where val type is *decimal.Decimal
// This is particularly useful for Snowflake because we're writing all the values as STRINGS into TSV format.
// This function guarantees backwards compatibility.
func (d *Decimal) String() string {
	targetExponent := -int32(d.scale)
	value := d.value
	if value.Exponent != targetExponent {
		value = numbers.DecimalWithNewExponent(value, targetExponent)
	}
	return value.Text('f')
}

// SnowflakeKind - is used to determine whether a NUMERIC data type should be a STRING or NUMERIC(p, s).
func (d *Decimal) SnowflakeKind() string {
	return d.toKind(MaxPrecisionBeforeString, "STRING")
}

// MsSQLKind - Has the same limitation as Redshift
// Spec: https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-ver16#arguments
func (d *Decimal) MsSQLKind() string {
	return d.toKind(MaxPrecisionBeforeString, "TEXT")
}

// RedshiftKind - is used to determine whether a NUMERIC data type should be a TEXT or NUMERIC(p, s).
func (d *Decimal) RedshiftKind() string {
	return d.toKind(MaxPrecisionBeforeString, "TEXT")
}

// BigQueryKind - is inferring logic from: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
func (d *Decimal) BigQueryKind() string {
	if d.isNumeric() {
		return fmt.Sprintf("NUMERIC(%v, %v)", *d.precision, d.scale)
	} else if d.isBigNumeric() {
		return fmt.Sprintf("BIGNUMERIC(%v, %v)", *d.precision, d.scale)
	}

	return "STRING"
}
