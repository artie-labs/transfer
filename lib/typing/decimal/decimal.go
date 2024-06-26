package decimal

import (
	"fmt"
	"log/slog"
	"math/big"

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
		value = DecimalWithNewExponent(value, targetExponent)
	}
	return value.Text('f')
}

func (d *Decimal) Value() any {
	stringValue := d.String()

	// -1 precision is used for variable scaled decimal
	// We are opting to emit this as a STRING because the value is technically unbounded (can get to ~1 GB).
	if d.precision != nil && (*d.precision > MaxPrecisionBeforeString || *d.precision == -1) {
		return stringValue
	}

	// Depending on the precision, we will want to convert value to STRING or keep as a FLOAT.
	bigFloat, ok := new(big.Float).SetString(stringValue)
	if !ok {
		slog.Error("Unable to parse value to a big.Float", slog.String("value", stringValue))
		return stringValue
	}

	return bigFloat
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

// DecimalWithNewExponent takes a [apd.Decimal] and returns a new [apd.Decimal] with a the given exponent.
// If the new exponent is less precise then the extra digits will be truncated.
func DecimalWithNewExponent(decimal *apd.Decimal, newExponent int32) *apd.Decimal {
	exponentDelta := newExponent - decimal.Exponent // Exponent is negative.

	if exponentDelta == 0 {
		return new(apd.Decimal).Set(decimal)
	}

	coefficient := new(apd.BigInt).Set(&decimal.Coeff)

	if exponentDelta < 0 {
		multiplier := new(apd.BigInt).Exp(apd.NewBigInt(10), apd.NewBigInt(int64(-exponentDelta)), nil)
		coefficient.Mul(coefficient, multiplier)
	} else if exponentDelta > 0 {
		divisor := new(apd.BigInt).Exp(apd.NewBigInt(10), apd.NewBigInt(int64(exponentDelta)), nil)
		coefficient.Div(coefficient, divisor)
	}

	return &apd.Decimal{
		Form:     decimal.Form,
		Negative: decimal.Negative,
		Exponent: newExponent,
		Coeff:    *coefficient,
	}
}
