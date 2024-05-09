package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

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

func (BigQueryDialect) KindForDataType(_type string, _ string) typing.KindDetails {
	return typing.BigQueryTypeToKind(_type)
}

func (BigQueryDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Error ends up looking like something like this: Column already exists: _string at [1:39]
	return strings.Contains(err.Error(), "Column already exists")
}

func (BigQueryDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) OPTIONS (expiration_timestamp = TIMESTAMP("%s"))`,
		fqTableName, strings.Join(colSQLParts, ","), typing.ExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)))
}

func (BigQueryDialect) BuildProcessToastStructColExpression(colName string) string {
	return fmt.Sprintf(`CASE WHEN COALESCE(TO_JSON_STRING(cc.%s) != '{"key":"%s"}', true) THEN cc.%s ELSE c.%s END`,
		colName, constants.ToastUnavailableValuePlaceholder,
		colName, colName)
}
