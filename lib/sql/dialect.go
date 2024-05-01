package sql

import (
	"fmt"
	"log/slog"
	"strings"
)

type Dialect interface {
	QuoteIdentifier(identifier string) string
}

type DefaultDialect struct{}

func (DefaultDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}

type BigQueryDialect struct{}

func (BigQueryDialect) QuoteIdentifier(identifier string) string {
	// BigQuery needs backticks to quote.
	return fmt.Sprintf("`%s`", identifier)
}

type RedshiftDialect struct{}

func (rd RedshiftDialect) QuoteIdentifier(identifier string) string {
	// Preserve the existing behavior of Redshift identifiers being lowercased due to not being quoted.
	return fmt.Sprintf(`"%s"`, strings.ToLower(identifier))
}

type SnowflakeDialect struct {
	UppercaseEscNames bool
}

func (sd SnowflakeDialect) QuoteIdentifier(identifier string) string {
	if sd.UppercaseEscNames {
		identifier = strings.ToUpper(identifier)
	} else {
		slog.Warn("Escaped Snowflake identifier is not being uppercased",
			slog.String("name", identifier),
			slog.Bool("uppercaseEscapedNames", sd.UppercaseEscNames),
		)
	}

	return fmt.Sprintf(`"%s"`, identifier)
}
