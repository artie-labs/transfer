package typing

import (
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func ParseNumeric(parameters []string) KindDetails {
	if len(parameters) == 0 || len(parameters) > 2 {
		return Invalid
	}

	var parsedNumbers []int
	for _, part := range parameters {
		parsedNumber, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return Invalid
		}

		parsedNumbers = append(parsedNumbers, parsedNumber)
	}

	// If scale is 0 or not specified, then number is an int.
	if len(parsedNumbers) == 1 || parsedNumbers[1] == 0 {
		return Integer
	}

	eDec := EDecimal
	eDec.ExtendedDecimalDetails = decimal.NewDecimal(&parsedNumbers[0], parsedNumbers[1], nil)
	return eDec
}
