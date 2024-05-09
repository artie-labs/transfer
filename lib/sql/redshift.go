package sql

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

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

func (RedshiftDialect) KindForDataType(_type string, stringPrecision string) (typing.KindDetails, error) {
	return typing.RedshiftTypeToKind(_type, stringPrecision), nil
}

func (RedshiftDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Redshift's error: ERROR: column "foo" of relation "statement" already exists
	return strings.Contains(err.Error(), "already exists")
}

func (RedshiftDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", fqTableName, strings.Join(colSQLParts, ","))
}

func (RedshiftDialect) BuildProcessToastStructColExpression(colName string) string {
	return fmt.Sprintf(`CASE WHEN COALESCE(cc.%s != JSON_PARSE('{"key":"%s"}'), true) THEN cc.%s ELSE c.%s END`,
		colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
}
