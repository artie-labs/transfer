package sql

import (
	"github.com/artie-labs/transfer/lib/config/constants"
)

func EscapeNameIfNecessary(name string, uppercaseEscNames bool, destKind constants.DestinationKind) string {
	// TODO: Switch all calls of [EscapeNameIfNecessary] to [EscapeNameIfNecessaryUsingDialect] and kill this.
	var dialect = dialectFor(destKind, uppercaseEscNames)

	if destKind != constants.S3 && dialect.NeedsEscaping(name) {
		return dialect.QuoteIdentifier(name)
	}
	return name
}

func EscapeNameIfNecessaryUsingDialect(name string, dialect Dialect) string {
	if dialect.NeedsEscaping(name) {
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
