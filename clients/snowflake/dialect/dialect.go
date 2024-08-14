package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type SnowflakeDialect struct{}

func (sd SnowflakeDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ToUpper(identifier))
}

func (SnowflakeDialect) EscapeStruct(value string) string {
	return sql.QuoteLiteral(value)
}

func (SnowflakeDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool) string {
	switch kindDetails.Kind {
	case typing.Struct.Kind:
		// Snowflake doesn't recognize struct.
		// Must be either OBJECT or VARIANT. However, VARIANT is more versatile.
		return "variant"
	case typing.Boolean.Kind:
		return "boolean"
	case typing.ETime.Kind:
		switch kindDetails.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			// We are not using `TIMESTAMP_NTZ` because Snowflake does not join on this data very well.
			// It ends up trying to parse this data into a TIMESTAMP_TZ and messes with the join order.
			// Specifically, if my location is in SF, it'll try to parse TIMESTAMP_NTZ into PST then into UTC.
			// When it was already stored as UTC.
			return "timestamp_tz"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.SnowflakeKind()
	}

	return kindDetails.Kind
}

// KindForDataType converts a Snowflake type to a KindDetails.
// Following this spec: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
func (SnowflakeDialect) KindForDataType(snowflakeType string, _ string) (typing.KindDetails, error) {
	if len(snowflakeType) == 0 {
		return typing.Invalid, nil
	}

	// We need to strip away the variable
	// For example, a Column can look like: TEXT, or Number(38, 0) or VARCHAR(255).
	// We need to strip out all the content from ( ... )
	dataType, parameters, err := sql.ParseDataTypeDefinition(strings.ToLower(snowflakeType))
	if err != nil {
		return typing.Invalid, err
	}

	// Geography, geometry date, time, varbinary, binary are currently not supported.
	switch dataType {
	case "number", "numeric":
		return typing.ParseNumeric(parameters), nil
	case "decimal":
		return typing.EDecimal, nil
	case "float", "float4",
		"float8", "double", "double precision", "real":
		return typing.Float, nil
	case "int", "integer", "bigint", "smallint", "tinyint", "byteint":
		return typing.Integer, nil
	case "varchar", "char", "character", "string", "text":
		switch len(parameters) {
		case 0:
			return typing.String, nil
		case 1:
			precision, err := strconv.ParseInt(parameters[0], 10, 32)
			if err != nil {
				return typing.Invalid, fmt.Errorf("unable to convert type parameter to an int: %w", err)
			}

			return typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: ptr.ToInt32(int32(precision)),
			}, nil
		default:
			return typing.Invalid, fmt.Errorf("expected at most one type parameters, received %d", len(parameters))
		}
	case "boolean":
		return typing.Boolean, nil
	case "variant", "object":
		return typing.Struct, nil
	case "array":
		return typing.Array, nil
	case "datetime", "timestamp", "timestamp_ltz", "timestamp_ntz", "timestamp_tz":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType), nil
	case "time":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType), nil
	case "date":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType), nil
	default:
		return typing.Invalid, nil
	}
}

func (SnowflakeDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Snowflake doesn't have column mutations (IF NOT EXISTS)
	return strings.Contains(err.Error(), "already exists")
}

// IsTableDoesNotExistErr will check if the resulting error message looks like this
// Table 'DATABASE.SCHEMA.TABLE' does not exist or not authorized. (resulting error message from DESC table)
func (SnowflakeDialect) IsTableDoesNotExistErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "does not exist or not authorized")
}

func (SnowflakeDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string) string {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))

	if temporary {
		// TEMPORARY Table syntax - https://docs.snowflake.com/en/sql-reference/sql/create-table
		// PURGE syntax - https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#purging-files-after-loading
		// FIELD_OPTIONALLY_ENCLOSED_BY - is needed because CSV will try to escape any values that have `"`
		return query + ` STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE)`
	} else {
		return query
	}
}

func (SnowflakeDialect) BuildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	return fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}

func (sd SnowflakeDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	colName := sql.QuoteTableAliasColumn(tableAlias, column, sd)
	if column.KindDetails == typing.Struct {
		return fmt.Sprintf("COALESCE(%s != {'key': '%s'}, true)", colName, constants.ToastUnavailableValuePlaceholder)
	}
	return fmt.Sprintf("COALESCE(%s != '%s', true)", colName, constants.ToastUnavailableValuePlaceholder)
}

func (SnowflakeDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	panic("not implemented")
}

func (sd SnowflakeDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, sd)

	orderColsToIterate := primaryKeysEscaped
	if includeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, sd.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, pk := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s ASC", pk))
	}

	var parts []string
	parts = append(parts, fmt.Sprintf("CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2)",
		stagingTableID.FullyQualifiedName(),
		tableID.FullyQualifiedName(),
		strings.Join(primaryKeysEscaped, ", "),
		strings.Join(orderByCols, ", "),
	))

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses, fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	parts = append(parts,
		fmt.Sprintf("DELETE FROM %s t1 USING %s t2 WHERE %s",
			tableID.FullyQualifiedName(),
			stagingTableID.FullyQualifiedName(),
			strings.Join(whereClauses, " AND "),
		),
	)

	parts = append(parts, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName()))
	return parts
}

func (sd SnowflakeDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	_ bool,
) ([]string, error) {
	equalitySQLParts := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, sd)
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
			// Update + soft deletion when we have previous values (update all columns)
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, sd), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, sd),
			// Soft deletion when we don't have previous values (only update the __artie_delete column)
			sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, sd), sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, sd),
			// Insert
			strings.Join(sql.QuoteColumns(cols, sd), ","),
			strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, sd), ","),
		)}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	cols, err = columns.RemoveDeleteColumnMarker(cols)
	if err != nil {
		return []string{}, err
	}

	return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND %s THEN DELETE
WHEN MATCHED AND IFNULL(%s, false) = false THEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s);`,
		// Delete
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, sd),
		// Update
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, sd), sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, sd),
		// Insert
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, sd), strings.Join(sql.QuoteColumns(cols, sd), ","),
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, sd), ","),
	)}, nil
}

func (SnowflakeDialect) BuildSweepQuery(dbName, schemaName string) (string, []any) {
	return fmt.Sprintf(`
SELECT
    table_schema, table_name
FROM
    %s.information_schema.tables
WHERE
    UPPER(table_schema) = UPPER(?) AND table_name ILIKE ?`, dbName), []any{schemaName, "%" + constants.ArtiePrefix + "%"}
}
