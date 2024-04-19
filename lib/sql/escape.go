package sql

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

type NameArgs struct {
	Escape   bool
	DestKind constants.DestinationKind
}

// symbolsToEscape are additional keywords that we need to escape
var symbolsToEscape = []string{":"}

func EscapeName(name string, uppercaseEscNames bool, args *NameArgs) string {
	if args == nil || !args.Escape {
		return name
	}

	var reservedKeywords []string
	if args.DestKind == constants.Redshift {
		reservedKeywords = constants.RedshiftReservedKeywords
	} else if args.DestKind == constants.MSSQL {
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

	if needsEscaping {
		if uppercaseEscNames {
			name = strings.ToUpper(name)
		}

		if args.DestKind == constants.BigQuery {
			// BigQuery needs backticks to escape.
			return fmt.Sprintf("`%s`", name)
		} else {
			// Snowflake uses quotes.
			return fmt.Sprintf(`"%s"`, name)
		}
	}

	return name
}
