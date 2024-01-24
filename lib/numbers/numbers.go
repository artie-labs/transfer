package numbers

import (
	"fmt"
	"strconv"
	"strings"
)

type BetweenEqArgs struct {
	Start  int
	End    int
	Number int
}

// BetweenEq - Looks something like this. start <= number <= end
func BetweenEq(args BetweenEqArgs) bool {
	return args.Number >= args.Start && args.Number <= args.End
}

func Float64ToString(f float64) string {
	// Convert float to string with maximum precision.
	str := strconv.FormatFloat(f, 'f', -1, 64)

	// Find the decimal point.
	dotIndex := strings.Index(str, ".")
	if dotIndex == -1 {
		// No decimal point, meaning it's an integer representation.
		return str
	}

	// Count the number of decimal places.
	decimalPlaces := len(str) - dotIndex - 1

	// Format the float with the exact number of decimal places.
	return fmt.Sprintf("%.*f", decimalPlaces, f)
}
