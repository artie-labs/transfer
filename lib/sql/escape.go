package sql

import (
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

// symbolsToEscape are additional keywords that we need to escape
var symbolsToEscape = []string{":"}

func EscapeNameIfNecessary(name string, uppercaseEscNames bool, destKind constants.DestinationKind) string {
	if NeedsEscaping(name, destKind) {
		return EscapeName(name, uppercaseEscNames, destKind)
	}
	return name
}

func NeedsEscaping(name string, destKind constants.DestinationKind) bool {
	var reservedKeywords []string
	if destKind == constants.Redshift {
		reservedKeywords = constants.RedshiftReservedKeywords
	} else if destKind == constants.MSSQL || destKind == constants.BigQuery {
		// TODO: Escape names that start with [constants.ArtiePrefix].
		if !strings.HasPrefix(name, constants.ArtiePrefix) {
			return true
		}
	} else {
		reservedKeywords = constants.ReservedKeywords
	}

	if slices.Contains(reservedKeywords, name) {
		return true
	}

	// If it does not contain any reserved words, does it contain any symbols that need to be escaped?
	for _, symbol := range symbolsToEscape {
		if strings.Contains(name, symbol) {
			return true
		}
	}

	// If it still doesn't need to be escaped, we should check if it's a number.
	if _, err := strconv.Atoi(name); err == nil {
		return true
	}

	return false
}

func EscapeName(name string, uppercaseEscNames bool, destKind constants.DestinationKind) string {
	if destKind == constants.Snowflake {
		if uppercaseEscNames {
			name = strings.ToUpper(name)
		} else {
			slog.Warn("Escaped Snowflake identifier is not being uppercased",
				slog.String("name", name),
				slog.Bool("uppercaseEscapedNames", uppercaseEscNames),
			)
		}
	}

	if destKind == constants.BigQuery {
		// BigQuery needs backticks to escape.
		return fmt.Sprintf("`%s`", name)
	} else {
		// Snowflake uses quotes.
		return fmt.Sprintf(`"%s"`, name)
	}
}
