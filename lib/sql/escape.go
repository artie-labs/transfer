package sql

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

// symbolsToEscape are additional keywords that we need to escape
var symbolsToEscape = []string{":"}

func EscapeNameIfNecessary(name string, uppercaseEscNames bool, destKind constants.DestinationKind) string {
	if needsEscaping(name, destKind) {
		return escapeName(name, uppercaseEscNames, destKind)
	}
	return name
}

func needsEscaping(name string, destKind constants.DestinationKind) bool {
	var reservedKeywords []string
	if destKind == constants.Redshift {
		reservedKeywords = constants.RedshiftReservedKeywords
	} else if destKind == constants.MSSQL {
		reservedKeywords = constants.MSSQLReservedKeywords
	} else {
		reservedKeywords = constants.ReservedKeywords
	}

	needsEscaping := slices.Contains(reservedKeywords, name)

	// If it does not contain any reserved words, does it contain any symbols that need to be escaped?
	if !needsEscaping {
		for _, symbol := range symbolsToEscape {
			if strings.Contains(name, symbol) {
				needsEscaping = true
				break
			}
		}
	}

	// If it still doesn't need to be escaped, we should check if it's a number.
	if !needsEscaping {
		if _, err := strconv.Atoi(name); err == nil {
			needsEscaping = true
		}
	}

	return needsEscaping
}

func escapeName(name string, uppercaseEscNames bool, destKind constants.DestinationKind) string {
	if uppercaseEscNames {
		name = strings.ToUpper(name)
	}

	if destKind == constants.BigQuery {
		// BigQuery needs backticks to escape.
		return fmt.Sprintf("`%s`", name)
	} else {
		// Snowflake uses quotes.
		return fmt.Sprintf(`"%s"`, name)
	}
}
