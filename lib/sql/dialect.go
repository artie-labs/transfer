package sql

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

type TableIdentifier interface {
	EscapedTable() string
	Table() string
	WithTable(table string) TableIdentifier
	FullyQualifiedName() string
}

type Dialect interface {
	QuoteIdentifier(identifier string) string
	EscapeStruct(value string) string
	DataTypeForKind(kd typing.KindDetails, isPk bool) string
	KindForDataType(_type string, stringPrecision string) (typing.KindDetails, error)
	IsColumnAlreadyExistsErr(err error) bool
	IsTableDoesNotExistErr(err error) bool
	BuildCreateTableQuery(fqTableName string, temporary bool, colSQLParts []string) string
	BuildAlterColumnQuery(fqTableName string, columnOp constants.ColumnOperation, colSQLPart string) string
	BuildProcessToastStructColExpression(colName string) string
}
