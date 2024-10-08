package typing

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func ParseNumeric(parts []string) (KindDetails, error) {
	if len(parts) == 0 || len(parts) > 2 {
		return Invalid, fmt.Errorf("invalid number of parts: %d", len(parts))
	}

	var parsedNumbers []int32
	for _, part := range parts {
		parsedNumber, err := strconv.ParseInt(strings.TrimSpace(part), 10, 32)
		if err != nil {
			return Invalid, fmt.Errorf("failed to parse number: %w", err)
		}

		parsedNumbers = append(parsedNumbers, int32(parsedNumber))
	}

	// If scale is 0 or not specified, then number is an int.
	if len(parsedNumbers) == 1 || parsedNumbers[1] == 0 {
		return NewDecimalDetailsFromTemplate(
			EDecimal,
			decimal.NewDetails(parsedNumbers[0], 0),
		), nil
	}

	return NewDecimalDetailsFromTemplate(
		EDecimal,
		decimal.NewDetails(parsedNumbers[0], parsedNumbers[1]),
	), nil
}
