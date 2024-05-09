package sql

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing"
)

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

func (MSSQLDialect) KindForDataType(_type string, stringPrecision string) typing.KindDetails {
	return typing.MSSQLTypeToKind(_type, stringPrecision)
}

func (MSSQLDialect) IsColumnAlreadyExistsErr(err error) bool {
	alreadyExistErrs := []string{
		// Column names in each table must be unique. Column name 'first_name' in table 'users' is specified more than once.
		"Column names in each table must be unique",
		// There is already an object named 'customers' in the database.
		"There is already an object named",
	}

	for _, alreadyExistErr := range alreadyExistErrs {
		if alreadyExist := strings.Contains(err.Error(), alreadyExistErr); alreadyExist {
			return alreadyExist
		}
	}

	return false
}

func (MSSQLDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	return fmt.Sprintf("CREATE TABLE %s (%s);", fqTableName, strings.Join(colSQLParts, ","))
}
