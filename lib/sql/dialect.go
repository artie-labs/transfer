package sql

import (
	"github.com/artie-labs/transfer/lib/typing"
)

type Dialect interface {
	QuoteIdentifier(identifier string) string
	EscapeStruct(value string) string
	DataTypeForKind(kd typing.KindDetails, isPk bool) string
	KindForDataType(_type string, stringPrecision string) (typing.KindDetails, error)
	SupportsColumnKeyword() bool
	IsColumnAlreadyExistsErr(err error) bool
	BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string
	BuildProcessToastStructColExpression(colName string) string
}
