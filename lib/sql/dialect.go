package sql

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing"
)

type Dialect interface {
	QuoteIdentifier(identifier string) string
	EscapeStruct(value string) string
	DataTypeForKind(kd typing.KindDetails, isPk bool) string
}

type BigQueryDialect struct{}

func (BigQueryDialect) QuoteIdentifier(identifier string) string {
	// BigQuery needs backticks to quote.
	return fmt.Sprintf("`%s`", identifier)
}

func (BigQueryDialect) EscapeStruct(value string) string {
	return "JSON" + QuoteLiteral(value)
}

func (BigQueryDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	return typing.KindToBigQuery(kd)
}

type MSSQLDialect struct{}

func (MSSQLDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}

func (MSSQLDialect) EscapeStruct(value string) string {
	panic("not implemented") // We don't currently support backfills for MS SQL.
}

func (MSSQLDialect) DataTypeForKind(kd typing.KindDetails, isPk bool) string {
	return typing.KindToMSSQL(kd, isPk)
}

type RedshiftDialect struct{}

func (rd RedshiftDialect) QuoteIdentifier(identifier string) string {
	// Preserve the existing behavior of Redshift identifiers being lowercased due to not being quoted.
	return fmt.Sprintf(`"%s"`, strings.ToLower(identifier))
}

func (RedshiftDialect) EscapeStruct(value string) string {
	return fmt.Sprintf("JSON_PARSE(%s)", QuoteLiteral(value))
}

func (RedshiftDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	return typing.KindToRedshift(kd)
}

type SnowflakeDialect struct{}

func (sd SnowflakeDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ToUpper(identifier))
}

func (SnowflakeDialect) EscapeStruct(value string) string {
	return QuoteLiteral(value)
}

func (SnowflakeDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	return typing.KindToSnowflake(kd)
}
