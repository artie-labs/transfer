package sql

import (
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
		return true
	case constants.S3:
		return false
	case constants.Snowflake:
		if uppercaseEscNames {
			// If uppercaseEscNames is true then we will escape all identifiers that do not start with the Artie priefix.
			// Since they will be uppercased afer they are escaped then they will result in the same value as if we
			// we were to use them in a query without any escaping at all.
			return true
		} else {
			if slices.Contains(constants.ReservedKeywords, name) {
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
		}
	default:
		slog.Error("Unsupported destination kind", slog.String("destKind", string(destKind)))
		return true
	}

	return false
}

func EscapeName(name string, uppercaseEscNames bool, destKind constants.DestinationKind) string {
	var dialect Dialect

	switch destKind {
	case constants.BigQuery:
		dialect = BigQueryDialect{}
	case constants.Snowflake:
		dialect = SnowflakeDialect{UppercaseEscNames: uppercaseEscNames}
	case constants.Redshift:
		dialect = RedshiftDialect{}
	default:
		dialect = DefaultDialect{}
	}

	return dialect.QuoteIdentifier(name)
}
