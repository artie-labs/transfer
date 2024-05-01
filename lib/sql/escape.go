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
	if NeedsEscaping(name, uppercaseEscNames, destKind) {
		return EscapeName(name, uppercaseEscNames, destKind)
	}
	return name
}

func NeedsEscaping(name string, uppercaseEscNames bool, destKind constants.DestinationKind) bool {
	switch destKind {
	case constants.BigQuery, constants.MSSQL, constants.Redshift:
		// TODO: Escape names that start with [constants.ArtiePrefix].
		if !strings.HasPrefix(name, constants.ArtiePrefix) {
			return true
		}
	case constants.S3:
		return false
	case constants.Snowflake:
		if uppercaseEscNames {
			// If uppercaseEscNames is true then we will escape all identifiers that do not start with the Artie priefix.
			// Since they will be uppercased afer they are escaped then they will result in the same value as if we
			// we were to use them in a query without any escaping at all.
			// TODO: Escape names that start with [constants.ArtiePrefix].
			if !strings.HasPrefix(name, constants.ArtiePrefix) {
				return true
			}
		} else {
			if slices.Contains(constants.ReservedKeywords, name) {
				return true
			}
			// If it still doesn't need to be escaped, we should check if it's a number.
			if _, err := strconv.Atoi(name); err == nil {
				return true
			}
		}
	default:
		slog.Error("Unsupported destination kind", slog.String("destKind", string(destKind)))
		return true
	}

	// If it does not contain any reserved words, does it contain any symbols that need to be escaped?
	for _, symbol := range symbolsToEscape {
		if strings.Contains(name, symbol) {
			return true
		}
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
	} else if destKind == constants.Redshift {
		// Preserve the existing behavior of Redshift identifiers being lowercased due to not being quoted.
		name = strings.ToLower(name)
	}

	if destKind == constants.BigQuery {
		// BigQuery needs backticks to escape.
		return fmt.Sprintf("`%s`", name)
	} else {
		// Everything else uses quotes.
		return fmt.Sprintf(`"%s"`, name)
	}
}
