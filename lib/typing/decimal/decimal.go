package decimal

import (
	"fmt"
	"math/big"
)

// Decimal is Artie's wrapper around *big.Float which can store large numbers w/ no precision loss.
type Decimal struct {
	scale     int
	precision int
	value     *big.Float
}

// MaxPrecisionBeforeString - if the precision is greater than 38, we'll cast it as a string.
// This is because Snowflake and BigQuery both do not have NUMERIC data types that go beyond 38.
const MaxPrecisionBeforeString = 38

// MaxScaleBeforeStringBigQuery - when scale exceeds 9, we'll set this to a STRING.
// Anything above 9 will exceed the NUMERIC data type in BigQuery, ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
const MaxScaleBeforeStringBigQuery = 9

func NewDecimal(scale, precision int, value *big.Float) *Decimal {
	if scale > precision && precision != -1 {
		// Note: -1 precision means it's not specified.

		// This is typically not possible, but Postgres has a design flaw that allows you to do things like: NUMERIC(5, 6) which actually equates to NUMERIC(5, 7)
		// We are adding `2` to precision because we'll need to account for the leading zero and decimal point.
		precision += 2
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

func (d *Decimal) Precision() int {
	return d.precision
}

func (d *Decimal) String() string {
	return d.value.Text('f', d.scale)
}

func (d *Decimal) Value() interface{} {
	// -1 precision is used for variable scaled decimal
	// We are opting to emit this as a STRING because the value is technically unbounded (can get to ~1 GB).
	if d.precision > MaxPrecisionBeforeString || d.precision == -1 {
		return d.String()
	}

	// Depending on the precision, we will want to convert value to STRING or keep as a FLOAT.
	return d.value
}

func (d *Decimal) SnowflakeKind() string {
	if d.precision > MaxPrecisionBeforeString || d.precision == -1 {
		return "STRING"
	}

	return fmt.Sprintf("NUMERIC(%v, %v)", d.precision, d.scale)
}

func (d *Decimal) BigQueryKind() string {
	if d.precision > MaxPrecisionBeforeString || d.precision == -1 || d.scale > MaxScaleBeforeStringBigQuery {
		return "STRING"
	}

	return fmt.Sprintf("NUMERIC(%v, %v)", d.precision, d.scale)
}
