package sql

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
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

func BuildAlterColumnQuery(dialect Dialect, fqTableName string, columnOp constants.ColumnOperation, colSQLPart string) string {
	if dialect.SupportsColumnKeyword() {
		return fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTableName, columnOp, colSQLPart)
	} else {
		// MSSQL doesn't support the COLUMN keyword
		return fmt.Sprintf("ALTER TABLE %s %s %s", fqTableName, columnOp, colSQLPart)
	}
}
