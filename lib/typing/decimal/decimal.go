package decimal

import (
	"fmt"
	"math/big"
)

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
	if d.precision > MaxPrecisionBeforeString || d.precision == -1 || d.scale > d.precision {
		return d.String()
	}

	// Depending on the precision, we will want to convert value to STRING or keep as a FLOAT.
	return d.value
}

func (d *Decimal) SnowflakeKind() string {
	// TODO: document NUMERIC(p, s)
	if d.precision > MaxPrecisionBeforeString || d.precision == -1 || d.scale > d.precision {
		return "STRING"
	}

	return fmt.Sprintf("NUMERIC(%v, %v)", d.precision, d.scale)
}

func (d *Decimal) BigQueryKind() string {
	if d.precision > MaxPrecisionBeforeString || d.precision == -1 || d.scale > MaxScaleBeforeStringBigQuery || d.scale > d.precision {
		return "STRING"
	}

	return fmt.Sprintf("NUMERIC(%v, %v)", d.precision, d.scale)
}
