package decimal

import (
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/cockroachdb/apd/v3"
)

const PrecisionNotSpecified int32 = -1

// Decimal is Artie's wrapper around [*apd.Decimal] which can store large numbers w/ no precision loss.
type Decimal struct {
	precision int32
	value     *apd.Decimal
}

func (d Decimal) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func NewDecimalWithPrecision(value *apd.Decimal, precision int32) *Decimal {
	return &Decimal{
		precision: precision,
		value:     value,
	}
}

func NewDecimal(value *apd.Decimal) *Decimal {
	return NewDecimalWithPrecision(value, PrecisionNotSpecified)
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
	return NewDetails(d.precision, -d.value.Exponent)
}

func (d *Decimal) ToDecimal128() (decimal128.Num, error) {
	return decimal128.FromString(d.String(), d.precision, -d.value.Exponent)
}
