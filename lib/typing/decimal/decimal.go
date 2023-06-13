package decimal

import (
	"math/big"
)

type Decimal struct {
	scale     int
	precision int
	value     *big.Float
}

// If the precision is greater than 38, we'll cast it as a string.
// This is because Snowflake and BigQuery both do not have NUMERIC data types that go beyond 38.
const maxPrecisionBeforeString = 38

func NewDecimal(scale, precision int, value *big.Float) *Decimal {
	return &Decimal{
		scale:     scale,
		precision: precision,
		value:     value,
	}
}

func (d *Decimal) Scale() int {
	return d.scale
}

func (d *Decimal) Precision() int {
	return d.precision
}

func (d *Decimal) String() string {
	return d.value.Text('f', d.scale)
}

func (d *Decimal) Value() interface{} {
	// -1 precision is used for variable scaled decimal
	// We are opting to emit this as a STRING because the value is technically unbounded (can get to ~1 GB).
	if d.precision > maxPrecisionBeforeString || d.precision == -1 {
		return d.String()
	}

	// Depending on the precision, we will want to convert value to STRING or keep as a FLOAT.
	return d.value
}
