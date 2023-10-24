package decimal

import (
	"fmt"
	"math/big"

	"github.com/artie-labs/transfer/lib/numbers"

	"github.com/artie-labs/transfer/lib/ptr"
)

// Decimal is Artie's wrapper around *big.Float which can store large numbers w/ no precision loss.
type Decimal struct {
	scale     int
	precision *int
	value     *big.Float
}

const (
	DefaultScale          = 5
	PrecisionNotSpecified = -1
)

// MaxPrecisionBeforeString - if the precision is greater than 38, we'll cast it as a string.
// This is because Snowflake and BigQuery both do not have NUMERIC data types that go beyond 38.
const MaxPrecisionBeforeString = 38

func NewDecimal(scale int, precision *int, value *big.Float) *Decimal {
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
	return d.value.Text('f', d.scale)
}

func (d *Decimal) Value() interface{} {
	// -1 precision is used for variable scaled decimal
	// We are opting to emit this as a STRING because the value is technically unbounded (can get to ~1 GB).
	if d.precision != nil && (*d.precision > MaxPrecisionBeforeString || *d.precision == -1) {
		return d.String()
	}

	// Depending on the precision, we will want to convert value to STRING or keep as a FLOAT.
	return d.value
}

// SnowflakeKind - is used to determine whether a NUMERIC data type should be a STRING or NUMERIC(p, s).
func (d *Decimal) SnowflakeKind() string {
	precision := MaxPrecisionBeforeString
	if d.precision != nil {
		precision = *d.precision
	}

	if precision > MaxPrecisionBeforeString || precision == -1 {
		return "STRING"
	}

	return fmt.Sprintf("NUMERIC(%v, %v)", precision, d.scale)
}

// RedshiftKind - is used to determine whether a NUMERIC data type should be a STRING or NUMERIC(p, s).
// This has the same max precision of 38 digits like Snowflake.
func (d *Decimal) RedshiftKind() string {
	precision := MaxPrecisionBeforeString
	if d.precision != nil {
		precision = *d.precision
	}

	if precision > MaxPrecisionBeforeString || precision == -1 {
		return "TEXT"
	}

	return fmt.Sprintf("NUMERIC(%v, %v)", precision, d.scale)
}

// MaxScaleBeforeStringBigQuery - when scale exceeds 9, we'll set this to a STRING.
// Anything above 9 will exceed the NUMERIC data type in BigQuery, ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
const MaxScaleBeforeStringBigQuery = 9

func (d *Decimal) BigQueryKind() string {
	if d.precision == nil || *d.precision == -1 {
		return "STRING"
	}

	if d.scale > MaxScaleBeforeStringBigQuery {
		return "STRING"
	}

	if numbers.BetweenEq(numbers.MaxInt(1, d.scale), d.scale+29, *d.precision) {
		return fmt.Sprintf("NUMERIC(%v, %v)", d.precision, d.scale)
	} else {
		return "STRING"
	}
}
