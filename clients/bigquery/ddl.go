package bigquery

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/optimization"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *Store) alterTable(_ context.Context, fqTableName string, createTable bool, columnOp constants.ColumnOperation, cdcTime time.Time, cols ...typing.Column) error {
	tc := s.configMap.TableConfig(fqTableName)
	if tc == nil {
		return fmt.Errorf("tableConfig is empty when trying to alter table, tableName: %s", fqTableName)
	}

	var mutateCol []typing.Column
	var colSQLPart string
	var err error
	for _, col := range cols {
		if col.Kind == typing.Invalid {
			// Let's not modify the table if the column kind is invalid
			continue
		}

		if columnOp == constants.Delete && !tc.ShouldDeleteColumn(col.Name, cdcTime) {
			// Don't delete yet, we can evaluate when we consume more messages.
			continue
		}

		mutateCol = append(mutateCol, col)
		switch columnOp {
		case constants.Add:
			colSQLPart = fmt.Sprintf("%s %s", col.Name, typing.KindToBigQuery(col.Kind))
		case constants.Delete:
			colSQLPart = fmt.Sprintf("%s", col.Name)
		}

		// If the table does not exist, create it.
		sqlQuery := fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTableName, columnOp, colSQLPart)
		if createTable {
			sqlQuery = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fqTableName, colSQLPart)
			createTable = false
		}

		_, err = s.Exec(sqlQuery)
		if err != nil && ColumnAlreadyExistErr(err) {
			// Snowflake doesn't have column mutations (IF NOT EXISTS)
			err = nil
		} else if err != nil {
			return err
		}
	}

	if err == nil {
		tc.MutateInMemoryColumns(createTable, columnOp, mutateCol...)
	}

	return nil
}

func (s *Store) GetTableConfig(_ context.Context, tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	fqName := tableData.ToFqName(constants.BigQuery)
	tc := s.configMap.TableConfig(fqName)
	if tc != nil {
		return tc, nil
	}

	rows, err := s.Query(fmt.Sprintf("SELECT ddl FROM %s.INFORMATION_SCHEMA.TABLES where table_name = '%s' LIMIT 1;",
		tableData.Database, tableData.TableName))
	if err != nil {
		// The query will not fail if the table doesn't exist. It will simply return 0 rows.
		// It WILL fail if the dataset doesn't exist or if it encounters any other forms of error.
		return nil, err
	}

	var sqlRow string
	for rows != nil && rows.Next() {
		var row string
		err = rows.Scan(&row)
		if err != nil {
			return nil, err
		}

		sqlRow = row
		break
	}

	// Table doesn't exist if the information schema query returned nothing.
	tableConfig, err := parseSchemaQuery(sqlRow, len(sqlRow) == 0)
	if err != nil {
		return nil, err
	}

	tableConfig.DropDeletedColumns = tableData.DropDeletedColumns
	s.configMap.AddTableToConfig(fqName, tableConfig)
	return tableConfig, nil
}

// parseSchemaQuery is to parse out the results from this query: https://cloud.google.com/bigquery/docs/information-schema-tables#example_1
func parseSchemaQuery(row string, createTable bool) (*types.DwhTableConfig, error) {
	if createTable {
		return types.NewDwhTableConfig(nil, nil, createTable), nil
	}

	// TrimSpace only does the L + R side.
	ddlString := strings.TrimSpace(row)

	leftBracketIdx := strings.Index(ddlString, "(")
	if leftBracketIdx < 0 || (leftBracketIdx+1) > len(ddlString) {
		return nil, fmt.Errorf("malformed DDL string: %s", ddlString)
	}

	// Sometimes the DDL statement has OPTIONS, sometimes it doesn't.
	// The cases we need to care for:
	// 1) CREATE TABLE `foo` (col_1 string, col_2 string) OPTIONS (...);
	// 2) CREATE TABLE `foo` (col_1 string, col_2 string)OPTIONS (...);
	// 3) CREATE TABLE `foo` (col_1 string, col_2 string);
	optionsIdx := strings.Index(ddlString, "OPTIONS")
	if optionsIdx < 0 {
		// This means, optionsIdx doesn't exist, so let's defer to finding the end of the statement (;).
		optionsIdx = len(ddlString)
	}

	if optionsIdx < 0 {
		return nil, fmt.Errorf("malformed DDL string: missing options, %s", ddlString)
	}

	if leftBracketIdx == optionsIdx {
		return nil, fmt.Errorf("malformed DDL string: position of ( and options are the same, %s", ddlString)
	}

	ddlString = ddlString[leftBracketIdx+1 : optionsIdx]
	endOfStatement := strings.LastIndex(ddlString, ")")
	if endOfStatement < 0 || (endOfStatement-1) < 0 {
		return nil, fmt.Errorf("malformed DDL string: missing (, %s", ddlString)
	}

	tableToColumnTypes := make(map[string]typing.KindDetails)
	ddlString = ddlString[:endOfStatement]
	columnsToTypes := strings.Split(ddlString, ",")
	for _, colType := range columnsToTypes {
		// TrimSpace will clear spaces, \t, \n for both L+R side of the string.
		colType = strings.TrimSpace(colType)
		if colType == "" {
			// This is because the schema can have a trailing `,` at the end of the list.
			// BigQuery is inconsistent in this manner.
			continue
		}

		parts := strings.Split(colType, " ")
		if len(parts) < 2 {
			return nil, fmt.Errorf("unexpected colType, colType: %s, parts: %v", colType, parts)
		}

		tableToColumnTypes[parts[0]] = typing.BigQueryTypeToKind(strings.Join(parts[1:], " "))
	}

	return types.NewDwhTableConfig(tableToColumnTypes, nil, createTable), nil
}
