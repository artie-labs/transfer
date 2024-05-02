package sql

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

type Dialect interface {
	QuoteIdentifier(identifier string) string
	EscapeStruct(value string) string
}

type BigQueryDialect struct{}

func (BigQueryDialect) QuoteIdentifier(identifier string) string {
	// BigQuery needs backticks to quote.
	return fmt.Sprintf("`%s`", identifier)
}

func (BigQueryDialect) EscapeStruct(value string) string {
	return "JSON" + QuoteLiteral(value)
}

type MSSQLDialect struct{}

func (MSSQLDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}

func (MSSQLDialect) EscapeStruct(value string) string {
	panic("not implemented") // We don't currently support backfills for MS SQL.
}

type RedshiftDialect struct{}

func (rd RedshiftDialect) QuoteIdentifier(identifier string) string {
	// Preserve the existing behavior of Redshift identifiers being lowercased due to not being quoted.
	return fmt.Sprintf(`"%s"`, strings.ToLower(identifier))
}

func (RedshiftDialect) EscapeStruct(value string) string {
	return fmt.Sprintf("JSON_PARSE(%s)", QuoteLiteral(value))
}

type SnowflakeDialect struct {
	LegacyMode bool
}

func (sd SnowflakeDialect) legacyNeedsEscaping(name string) bool {
	return slices.Contains(constants.ReservedKeywords, name) || strings.Contains(name, ":")
}

func (sd SnowflakeDialect) QuoteIdentifier(identifier string) string {
	if sd.LegacyMode {
		if sd.legacyNeedsEscaping(identifier) {
			// In legacy mode we would have escaped this identifier which would have caused it to be lowercase.
			slog.Warn("Escaped Snowflake identifier is not being uppercased",
				slog.String("name", identifier),
			)
		} else {
			// Since this identifier wasn't previously escaped it will have been used uppercase.
			identifier = strings.ToUpper(identifier)
		}
	} else {
		identifier = strings.ToUpper(identifier)
	}

	return fmt.Sprintf(`"%s"`, identifier)
}

func (SnowflakeDialect) EscapeStruct(value string) string {
	return QuoteLiteral(value)
}
