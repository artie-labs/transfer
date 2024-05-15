package dialect

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type MSSQLDialect struct{}

func (MSSQLDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}

func (MSSQLDialect) EscapeStruct(value string) string {
	panic("not implemented") // We don't currently support backfills for MS SQL.
}

func (MSSQLDialect) DataTypeForKind(kindDetails typing.KindDetails, isPk bool) string {
	// Primary keys cannot exceed 900 chars in length.
	// https://learn.microsoft.com/en-us/sql/relational-databases/tables/primary-and-foreign-key-constraints?view=sql-server-ver16#PKeys
	const maxVarCharLengthForPrimaryKey = 900

	switch kindDetails.Kind {
	case typing.Float.Kind:
		return "float"
	case typing.Integer.Kind:
		return "bigint"
	case typing.Struct.Kind, typing.Array.Kind:
		return "NVARCHAR(MAX)"
	case typing.String.Kind:
		if kindDetails.OptionalStringPrecision != nil {
			precision := *kindDetails.OptionalStringPrecision
			if isPk {
				precision = min(maxVarCharLengthForPrimaryKey, precision)
			}

			return fmt.Sprintf("VARCHAR(%d)", precision)
		}

		if isPk {
			return fmt.Sprintf("VARCHAR(%d)", maxVarCharLengthForPrimaryKey)
		}

		return "VARCHAR(MAX)"
	case typing.Boolean.Kind:
		return "BIT"
	case typing.ETime.Kind:
		switch kindDetails.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			// Using datetime2 because it's the recommendation, and it provides more precision: https://stackoverflow.com/a/1884088
			return "datetime2"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.MsSQLKind()
	}

	return kindDetails.Kind
}

func (MSSQLDialect) KindForDataType(rawType string, stringPrecision string) (typing.KindDetails, error) {
	rawType = strings.ToLower(rawType)

	if strings.HasPrefix(rawType, "numeric") {
		_, parameters, err := sql.ParseDataTypeDefinition(rawType)
		if err != nil {
			return typing.Invalid, err
		}
		return typing.ParseNumeric(parameters), nil
	}

	switch rawType {
	case
		"char",
		"varchar",
		"nchar",
		"nvarchar",
		"ntext":
		var strPrecision *int
		precision, err := strconv.Atoi(stringPrecision)
		if err == nil {
			strPrecision = &precision
		}

		// precision of -1 means it's MAX.
		if precision == -1 {
			strPrecision = nil
		}

		return typing.KindDetails{
			Kind:                    typing.String.Kind,
			OptionalStringPrecision: strPrecision,
		}, nil
	case
		"smallint",
		"tinyint",
		"bigint",
		"int":
		return typing.Integer, nil
	case "float", "real":
		return typing.Float, nil
	case
		"datetime",
		"datetime2":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType), nil
	case "time":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType), nil
	case "date":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType), nil
	case "bit":
		return typing.Boolean, nil
	case "text":
		return typing.String, nil
	}

	return typing.Invalid, nil
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

func (MSSQLDialect) IsTableDoesNotExistErr(err error) bool {
	return false
}

func (MSSQLDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Microsoft SQL Server uses the same syntax for temporary and permanant tables.
	// Microsoft SQL Server doesn't support IF NOT EXISTS
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (MSSQLDialect) BuildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	// Microsoft SQL Server doesn't support the COLUMN keyword
	return fmt.Sprintf("ALTER TABLE %s %s %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}

func (md MSSQLDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	colName := md.QuoteIdentifier(column.Name())
	// Microsoft SQL Server doesn't allow boolean expressions to be in the COALESCE statement.
	if column.KindDetails == typing.Struct {
		return fmt.Sprintf("COALESCE(%s.%s, {}) != {'key': '%s'}", tableAlias, colName, constants.ToastUnavailableValuePlaceholder)
	}
	return fmt.Sprintf("COALESCE(%s.%s, '') != '%s'", tableAlias, colName, constants.ToastUnavailableValuePlaceholder)
}

func (MSSQLDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) []string {
	panic("not implemented") // We don't currently support deduping for MS SQL.
}

func (md MSSQLDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	idempotentKey string,
	primaryKeys []columns.Column,
	_ []string,
	cols []columns.Column,
	softDelete bool,
	_ bool,
) ([]string, error) {
	var idempotentClause string
	if idempotentKey != "" {
		idempotentClause = fmt.Sprintf("AND %s.%s >= %s.%s ", constants.StagingAlias, idempotentKey, constants.TargetAlias, idempotentKey)
	}

	baseQuery := fmt.Sprintf(`
MERGE INTO %s %s
USING %s AS %s ON %s`,
		tableID.FullyQualifiedName(), constants.TargetAlias,
		subQuery, constants.StagingAlias,
		strings.Join(sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, md), " AND "),
	)

	if softDelete {
		return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND COALESCE(%s.%s, 0) = 0 THEN INSERT (%s) VALUES (%s);`,
			// WHEN MATCHED %sTHEN UPDATE SET %s
			idempotentClause, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, md),
			// WHEN NOT MATCHED AND COALESCE(%s.%s, 0) = 0 THEN INSERT (%s)
			constants.StagingAlias, md.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(sql.QuoteColumns(cols, md), ","),
			// VALUES (%s);
			strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, md), ","),
		)}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	cols, removed := columns.RemoveDeleteColumnMarker(cols)
	if !removed {
		return nil, errors.New("artie delete flag doesn't exist")
	}

	return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND %s.%s = 1 THEN DELETE
WHEN MATCHED AND COALESCE(%s.%s, 0) = 0 %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND COALESCE(%s.%s, 1) = 0 THEN INSERT (%s) VALUES (%s);`,
		// WHEN MATCHED AND %s.%s = 1 THEN DELETE
		constants.StagingAlias, md.QuoteIdentifier(constants.DeleteColumnMarker),
		// WHEN MATCHED AND COALESCE(%s.%s, 0) = 0 %sTHEN UPDATE SET %s
		constants.StagingAlias, md.QuoteIdentifier(constants.DeleteColumnMarker), idempotentClause, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, md),
		// WHEN NOT MATCHED AND COALESCE(%s.%s, 1) = 0 THEN INSERT (%s)
		constants.StagingAlias, md.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(sql.QuoteColumns(cols, md), ","),
		// VALUES (%s);
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, md), ","),
	)}, nil
}
