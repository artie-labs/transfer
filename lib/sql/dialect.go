package sql

import (
	"fmt"
	"strings"
)

type Dialect interface {
	NeedsEscaping(identifier string) bool // TODO: Remove this when we escape everything
	QuoteIdentifier(identifier string) string
	EscapeStruct(value string) string
}

type BigQueryDialect struct{}

func (BigQueryDialect) NeedsEscaping(_ string) bool { return true }

func (BigQueryDialect) QuoteIdentifier(identifier string) string {
	// BigQuery needs backticks to quote.
	return fmt.Sprintf("`%s`", identifier)
}

func (BigQueryDialect) EscapeStruct(value string) string {
	return "JSON" + QuoteLiteral(value)
}

type MSSQLDialect struct{}

func (MSSQLDialect) NeedsEscaping(_ string) bool { return true }

func (MSSQLDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}

func (MSSQLDialect) EscapeStruct(value string) string {
	panic("not implemented") // We don't currently support backfills for MS SQL.
}

type RedshiftDialect struct{}

func (RedshiftDialect) NeedsEscaping(_ string) bool { return true }

func (rd RedshiftDialect) QuoteIdentifier(identifier string) string {
	// Preserve the existing behavior of Redshift identifiers being lowercased due to not being quoted.
	return fmt.Sprintf(`"%s"`, strings.ToLower(identifier))
}

func (RedshiftDialect) EscapeStruct(value string) string {
	return fmt.Sprintf("JSON_PARSE(%s)", QuoteLiteral(value))
}

type SnowflakeDialect struct{}

func (sd SnowflakeDialect) NeedsEscaping(name string) bool {
	return true
}

func (sd SnowflakeDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ToUpper(identifier))
}

func (SnowflakeDialect) EscapeStruct(value string) string {
	return QuoteLiteral(value)
}
