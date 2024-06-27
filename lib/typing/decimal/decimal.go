package decimal

import (
	"github.com/cockroachdb/apd/v3"
)

// Decimal is Artie's wrapper around [*apd.Decimal] which can store large numbers w/ no precision loss.
type Decimal struct {
	precision int32
	value     *apd.Decimal
}

const (
	DefaultScale          int32 = 5
	PrecisionNotSpecified int32 = -1
	// MaxPrecisionBeforeString - if the precision is greater than 38, we'll cast it as a string.
	// This is because Snowflake and BigQuery both do not have NUMERIC data types that go beyond 38.
	MaxPrecisionBeforeString int32 = 38
)

func NewDecimalWithPrecision(value *apd.Decimal, precision int32) *Decimal {
	scale := -value.Exponent
	if scale > precision && precision != PrecisionNotSpecified {
		// Note: -1 precision means it's not specified.

		// This is typically not possible, but Postgres has a design flaw that allows you to do things like: NUMERIC(5, 6) which actually equates to NUMERIC(7, 6)
		// We are setting precision to be scale + 1 to account for the leading zero for decimal numbers.
		precision = scale + 1
	}

	return &Decimal{
		precision: precision,
		value:     value,
	}
}

func NewDecimal(value *apd.Decimal) *Decimal {
	return NewDecimalWithPrecision(value, PrecisionNotSpecified)
}

func (d *Decimal) Scale() int32 {
	return -d.value.Exponent
}

func (d *Decimal) Precision() int32 {
	return d.precision
}

func (d *Decimal) Value() *apd.Decimal {
	return d.value
}

// String() is used to override fmt.Sprint(val), where val type is *decimal.Decimal
// This is particularly useful for Snowflake because we're writing all the values as STRINGS into TSV format.
// This function guarantees backwards compatibility.
func (d *Decimal) String() string {
	return d.value.Text('f')
}

func (d *Decimal) Details() Details {
	return Details{scale: d.Scale(), precision: d.precision}
}
