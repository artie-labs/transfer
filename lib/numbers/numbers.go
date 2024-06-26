package numbers

import "github.com/cockroachdb/apd/v3"

// BetweenEq - Looks something like this. start <= number <= end
func BetweenEq[T int | int32 | int64](start, end, number T) bool {
	return number >= start && number <= end
}

// MustParseDecimal parses a string to a [*apd.Decimal] or panics -- used for tests.
func MustParseDecimal(value string) *apd.Decimal {
	decimal, _, err := apd.NewFromString(value)
	if err != nil {
		panic(err)
	}
	return decimal
}
