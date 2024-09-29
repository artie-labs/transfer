package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type DatabricksDialect struct{}

func (d DatabricksDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func (d DatabricksDialect) EscapeStruct(value string) string {
	panic("not implemented") // We don't currently support backfills for Databricks.
}

func (d DatabricksDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	// https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
	switch kd.Kind {
	case typing.String.Kind:
		return "STRING"
	case typing.Integer.Kind:
		return "INT"
	case typing.Float.Kind:
		return "FLOAT"
	case typing.EDecimal.Kind:
		return kd.ExtendedDecimalDetails.SnowflakeKind()
	case typing.Boolean.Kind:
		return "BOOLEAN"
	case typing.ETime.Kind:
		switch kd.ExtendedTimeDetails.Type {
		case ext.TimestampTzKindType:
			return "TIMESTAMP"
		case ext.DateKindType:
			return "DATE"
		case ext.TimeKindType:
			// Databricks doesn't have an explicit TIME type, so we use STRING instead
			return "STRING"
		}
	case typing.Struct.Kind:
		return "VARIANT"
	case typing.Array.Kind:
		// This is because Databricks requires typing within the element of an array (similar to BigQuery).
		return "ARRAY<STRING>"
	}

	return kd.Kind
}

func (d DatabricksDialect) KindForDataType(_type string, _ string) (typing.KindDetails, error) {
	// TODO: Finish
	switch strings.ToUpper(_type) {
	case "STRING":
		return typing.String, nil
	case "INT":
		return typing.Integer, nil
	case "FLOAT":
		return typing.Float, nil
	case "BOOLEAN":
		return typing.Boolean, nil
	case "VARIANT":
		return typing.Struct, nil
	}

	return typing.KindDetails{}, fmt.Errorf("unsupported data type: %q", _type)
}

func (d DatabricksDialect) IsColumnAlreadyExistsErr(_ error) bool {
	return false
}

func (d DatabricksDialect) IsTableDoesNotExistErr(err error) bool {
	// Implement the logic to check if the error is a "table does not exist" error
	return strings.Contains(err.Error(), "does not exist")
}

func (d DatabricksDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string) string {
	temp := ""
	if temporary {
		temp = "TEMPORARY "
	}
	return fmt.Sprintf("CREATE %sTABLE %s (%s)", temp, tableID.FullyQualifiedName(), strings.Join(colSQLParts, ", "))
}

func (d DatabricksDialect) BuildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	return fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}

func (d DatabricksDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	return fmt.Sprintf("%s.%s IS NOT NULL", tableAlias, column.Name)
}

func (d DatabricksDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE ROWID NOT IN (SELECT MAX(ROWID) FROM %s GROUP BY %s)", tableID.FullyQualifiedName(), tableID.FullyQualifiedName(), strings.Join(primaryKeys, ", "))
}

func (d DatabricksDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	var queries []string
	queries = append(queries, fmt.Sprintf("CREATE OR REPLACE TEMPORARY VIEW %s AS SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1",
		stagingTableID.FullyQualifiedName(), tableID.FullyQualifiedName(), strings.Join(primaryKeys, ", "), "updated_at DESC"))
	queries = append(queries, fmt.Sprintf("DELETE FROM %s WHERE EXISTS (SELECT 1 FROM %s WHERE %s)", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName(), strings.Join(primaryKeys, " AND ")))
	queries = append(queries, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName()))
	return queries
}

func (d DatabricksDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	containsHardDeletes bool,
) ([]string, error) {
	equalitySQLParts := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, d)
	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}
	baseQuery := fmt.Sprintf(`
MERGE INTO %s %s USING ( %s ) AS %s ON %s`,
		tableID.FullyQualifiedName(), constants.TargetAlias, subQuery, constants.StagingAlias, strings.Join(equalitySQLParts, " AND "),
	)

	cols, err := columns.RemoveOnlySetDeleteColumnMarker(cols)
	if err != nil {
		return []string{}, err
	}

	if softDelete {
		return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN MATCHED AND IFNULL(%s, false) = true THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);`,
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, d), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, d),
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, d), sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, d),
			strings.Join(sql.QuoteColumns(cols, d), ","),
			strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, d), ","),
		)}, nil
	}

	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return []string{}, err
	}

	return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND %s THEN DELETE
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s);`,
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, d),
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, d), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, d),
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, d), strings.Join(sql.QuoteColumns(cols, d), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, d), ","),
	)}, nil
}

func (d DatabricksDialect) GetDefaultValueStrategy() sql.DefaultValueStrategy {
	return sql.Native
}
