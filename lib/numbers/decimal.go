package numbers

import "github.com/cockroachdb/apd/v3"

// MustParseDecimal parses a string to an [apd.Decimal] or panics -- used for tests.
func MustParseDecimal(value string) *apd.Decimal {
	decimal, _, err := apd.NewFromString(value)
	if err != nil {
		panic(err)
	}
	return decimal
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
