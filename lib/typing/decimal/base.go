package decimal

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/numbers"
)

func (d *Decimal) isNumeric() bool {
	if d.precision == nil || *d.precision == PrecisionNotSpecified {
		return false
	}

	// 0 <= s <= 9
	if !numbers.BetweenEq(numbers.BetweenEqArgs{Start: 0, End: 9, Number: d.scale}) {
		return false
	}

	// max(1,s) <= p <= s + 29
	return numbers.BetweenEq(numbers.BetweenEqArgs{
		Start:  max(1, d.scale),
		End:    d.scale + 29,
		Number: *d.precision,
	})
}

func (d *Decimal) isBigNumeric() bool {
	if d.precision == nil || *d.precision == -1 {
		return false
	}

	// 0 <= s <= 38
	if !numbers.BetweenEq(numbers.BetweenEqArgs{Start: 0, End: 38, Number: d.scale}) {
		return false
	}

	// max(1,s) <= p <= s + 38
	return numbers.BetweenEq(numbers.BetweenEqArgs{
		Start:  max(1, d.scale),
		End:    d.scale + 38,
		Number: *d.precision,
	})
}

func (d *Decimal) toKind(maxPrecision int, exceededKind string) string {
	precision := maxPrecision
	if d.precision != nil {
		precision = *d.precision
	}

	if precision > maxPrecision || precision == -1 {
		return exceededKind
	}

	return fmt.Sprintf("NUMERIC(%v, %v)", precision, d.scale)
}
