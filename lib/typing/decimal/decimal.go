package decimal

import (
	"log/slog"
	"math/big"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/cockroachdb/apd/v3"
)

// Decimal is Artie's wrapper around [*apd.Decimal] which can store large numbers w/ no precision loss.
type Decimal struct {
	precision *int32
	value     *apd.Decimal
}

const (
	DefaultScale          int32 = 5
	PrecisionNotSpecified int32 = -1
	// MaxPrecisionBeforeString - if the precision is greater than 38, we'll cast it as a string.
	// This is because Snowflake and BigQuery both do not have NUMERIC data types that go beyond 38.
	MaxPrecisionBeforeString int32 = 38
)

func NewDecimal(precision *int32, value *apd.Decimal) *Decimal {
	if precision != nil {
		scale := -value.Exponent
		if scale > *precision && *precision != PrecisionNotSpecified {
			// Note: -1 precision means it's not specified.

			// This is typically not possible, but Postgres has a design flaw that allows you to do things like: NUMERIC(5, 6) which actually equates to NUMERIC(7, 6)
			// We are setting precision to be scale + 1 to account for the leading zero for decimal numbers.
			precision = ptr.ToInt32(scale + 1)
		}
	}

	return &Decimal{
		precision: precision,
		value:     value,
	}
}

func (d *Decimal) Scale() int32 {
	return -d.value.Exponent
}

func (d *Decimal) Precision() *int32 {
	return d.precision
}

// String() is used to override fmt.Sprint(val), where val type is *decimal.Decimal
// This is particularly useful for Snowflake because we're writing all the values as STRINGS into TSV format.
// This function guarantees backwards compatibility.
func (d *Decimal) String() string {
	return d.value.Text('f')
}

func (d *Decimal) Value() any {
	// -1 precision is used for variable scaled decimal
	// We are opting to emit this as a STRING because the value is technically unbounded (can get to ~1 GB).
	if d.precision != nil && (*d.precision > MaxPrecisionBeforeString || *d.precision == PrecisionNotSpecified) {
		return d.String()
	}

	// Depending on the precision, we will want to convert value to STRING or keep as a FLOAT.
	// TODO: [Value] is only called in one place, look into calling [String] instead.
	if out, ok := new(big.Float).SetString(d.String()); ok {
		return out
	}
	slog.Error("Failed to convert apd.Decimal to big.Float", slog.String("value", d.String()))
	return d.String()
}

func (d *Decimal) Details() DecimalDetails {
	precision := PrecisionNotSpecified
	if d.precision != nil {
		precision = *d.precision
	}
	return DecimalDetails{scale: d.Scale(), precision: precision}
}
