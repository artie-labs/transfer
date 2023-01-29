package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *Store) alterTable(ctx context.Context, fqTableName string, createTable bool, columnOp config.ColumnOperation, cdcTime time.Time, cols ...typing.Column) error {
	fmt.Println("fqTableName", fqTableName)
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

		if columnOp == config.Delete && !tc.ShouldDeleteColumn(col.Name, cdcTime) {
			// Don't delete yet, we can evaluate when we consume more messages.
			continue
		}

		mutateCol = append(mutateCol, col)
		switch columnOp {
		case config.Add:
			colSQLPart = fmt.Sprintf("%s %s", col.Name, typing.KindToBigQuery(col.Kind))
		case config.Delete:
			colSQLPart = fmt.Sprintf("%s", col.Name)
		}

		// If the table does not exist, create it.
		sqlQuery := fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTableName, columnOp, colSQLPart)
		if createTable {
			sqlQuery = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fqTableName, colSQLPart)
			createTable = false
		}

		fmt.Println("sqlQuery", sqlQuery)

		_, err = s.c.Query(sqlQuery).Read(ctx)
		if err != nil && ColumnAlreadyExistErr(err) {
			// Snowflake doesn't have column mutations (IF NOT EXISTS)
			err = nil
		} else if err != nil {
			return err
		}
	}

	if err == nil {
		tc.MutateColumnsWithMemCache(createTable, columnOp, mutateCol...)
	}

	return nil
}

func (s *Store) GetTableConfig(ctx context.Context, dataset, table string) (*types.DwhTableConfig, error) {
	fqName := fmt.Sprintf("%s.%s", dataset, table)
	tc := s.configMap.TableConfig(fqName)
	if tc != nil {
		return tc, nil
	}

	//log := logger.FromContext(ctx)
	rows, err := s.c.Query(fmt.Sprintf("SELECT ddl FROM %s.INFORMATION_SCHEMA.TABLES where table_name = '%s' LIMIT 1;", dataset, table)).Read(ctx)
	if err != nil {
		// The query will not fail if the table doesn't exist. It will simply return 0 rows.
		// It WILL fail if the dataset doesn't exist or if it encounters any other forms of error.
		return nil, err
	}

	var sqlRow string
	for rows != nil {
		var row []bigquery.Value
		err = rows.Next(&row)
		if err == iterator.Done {
			// Done reading
			break
		} else if err != nil {
			return nil, err
		}

		if len(row) > 0 {
			// We only care about the first row.
			sqlRow = fmt.Sprint(row[0])
		}

		break
	}

	// Table doesn't exist if the information schema query returned nothing.
	tableConfig, err := ParseSchemaQuery(sqlRow, len(sqlRow) == 0)
	if err != nil {
		return nil, err
	}

	s.configMap.AddTableToConfig(fqName, tableConfig)
	return tableConfig, nil
}

// ParseSchemaQuery is to parse out the results from this query: https://cloud.google.com/bigquery/docs/information-schema-tables#example_1
func ParseSchemaQuery(row string, createTable bool) (*types.DwhTableConfig, error) {
	if createTable {
		return types.NewDwhTableConfig(nil, nil, createTable), nil
	}

	fmt.Println("row", row, "createTable", createTable)

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
		return nil, fmt.Errorf("malformed DDL string1, %s", ddlString)
	}

	if leftBracketIdx == optionsIdx {
		return nil, fmt.Errorf("malformed DDL string2, %s", ddlString)
	}

	ddlString = ddlString[leftBracketIdx+1 : optionsIdx]
	endOfStatement := strings.LastIndex(ddlString, ")")
	if endOfStatement < 0 || (endOfStatement-1) < 0 {
		return nil, fmt.Errorf("malformed DDL string3, %s", ddlString)
	}

	tableToColumnTypes := make(map[string]typing.Kind)
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
