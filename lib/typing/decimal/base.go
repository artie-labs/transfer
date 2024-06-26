package decimal

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/numbers"
)

func (d *DecimalDetails) isNumeric() bool {
	if d.precision == PrecisionNotSpecified {
		return false
	}

	// 0 <= s <= 9
	if !numbers.BetweenEq(0, 9, d.scale) {
		return false
	}

	// max(1,s) <= p <= s + 29
	return numbers.BetweenEq(max(1, d.scale), d.scale+29, d.precision)
}

func (d *DecimalDetails) isBigNumeric() bool {
	if d.precision == PrecisionNotSpecified {
		return false
	}

	// 0 <= s <= 38
	if !numbers.BetweenEq(0, 38, d.scale) {
		return false
	}

	// max(1,s) <= p <= s + 38
	return numbers.BetweenEq(max(1, d.scale), d.scale+38, d.precision)
}

func (d *DecimalDetails) toKind(maxPrecision int32, exceededKind string) string {
	if d.precision > maxPrecision || d.precision == PrecisionNotSpecified {
		return exceededKind
	}

	return fmt.Sprintf("NUMERIC(%v, %v)", d.precision, d.scale)
}
