package sql

import (
	"github.com/artie-labs/transfer/lib/config/constants"
)

// symbolsToEscape are additional keywords that we need to escape
var symbolsToEscape = []string{":"}

func EscapeNameIfNecessary(name string, uppercaseEscNames bool, destKind constants.DestinationKind) string {
	var dialect = dialectFor(destKind, uppercaseEscNames)

	if destKind != constants.S3 && dialect.NeedsEscaping(name) {
		return dialect.QuoteIdentifier(name)
	}
	return name
}

func dialectFor(destKind constants.DestinationKind, uppercaseEscNames bool) Dialect {
	switch destKind {
	case constants.BigQuery:
		return BigQueryDialect{}
	case constants.Snowflake:
		return SnowflakeDialect{UppercaseEscNames: uppercaseEscNames}
	case constants.Redshift:
		return RedshiftDialect{}
	default:
		return DefaultDialect{}
	}
}

func EscapeName(name string, uppercaseEscNames bool, destKind constants.DestinationKind) string {
	// TODO: This is only used in one place, remove once [Dialect] has beem added to [Store].
	return dialectFor(destKind, uppercaseEscNames).QuoteIdentifier(name)
}
