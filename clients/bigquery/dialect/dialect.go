package dialect

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

const (
	BQStreamingTimeFormat = "15:04:05"

	bqLayout = "2006-01-02 15:04:05 MST"
)

func BQExpiresDate(time time.Time) string {
	// BigQuery expects the timestamp to look in this format: 2023-01-01 00:00:00 UTC
	// This is used as part of table options.
	return time.Format(bqLayout)
}

type BigQueryDialect struct{}

func (BigQueryDialect) QuoteIdentifier(identifier string) string {
	// BigQuery needs backticks to quote.
	return fmt.Sprintf("`%s`", identifier)
}

func (BigQueryDialect) EscapeStruct(value string) string {
	return "JSON" + sql.QuoteLiteral(value)
}

func (BigQueryDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool) string {
	// Doesn't look like we need to do any special type mapping.
	switch kindDetails.Kind {
	case typing.Float.Kind:
		return "float64"
	case typing.Array.Kind:
		// This is because BigQuery requires typing within the element of an array
		// IMO, a string type is the least controversial data type (others being bool, number, struct).
		// With String, we can always type cast the child elements.
		// BQ does this because 2d+ arrays are not allowed. See: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_type
		return "array<string>"
	case typing.Struct.Kind:
		// Struct is a tighter version of JSON that requires type casting like Struct<int64>
		return "json"
	case typing.ETime.Kind:
		switch kindDetails.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type
			// We should be using TIMESTAMP since it's an absolute point in time.
			return "timestamp"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	case typing.EDecimal.Kind:
		return kindDetails.ExtendedDecimalDetails.BigQueryKind()
	}

	return kindDetails.Kind
}

func (BigQueryDialect) KindForDataType(rawBqType string, _ string) (typing.KindDetails, error) {
	if len(rawBqType) == 0 {
		return typing.Invalid, nil
	}

	bqType, parameters, err := sql.ParseDataTypeDefinition(strings.ToLower(rawBqType))
	if err != nil {
		return typing.Invalid, err
	}

	// Trim Struct<k type> to Struct
	idxStop := len(bqType)
	if idx := strings.Index(bqType, "<"); idx > 0 {
		idxStop = idx
	}

	// Geography, geometry date, time, varbinary, binary are currently not supported.
	switch strings.TrimSpace(bqType[:idxStop]) {
	case "numeric", "bignumeric":
		if len(parameters) == 0 {
			// This is a specific thing to BigQuery
			// A `NUMERIC` type without precision or scale specified is NUMERIC(38, 9)
			return typing.EDecimal, nil
		}
		return typing.ParseNumeric(parameters), nil
	case "decimal", "float", "float64", "bigdecimal":
		return typing.Float, nil
	case "int", "integer", "int64":
		return typing.Integer, nil
	case "varchar", "string":
		return typing.String, nil
	case "bool", "boolean":
		return typing.Boolean, nil
	case "struct", "json", "record":
		// Record is a legacy BQ object that maps to a JSON.
		return typing.Struct, nil
	case "array":
		return typing.Array, nil
	case "datetime", "timestamp":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType), nil
	case "time":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType), nil
	case "date":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType), nil
	default:
		return typing.Invalid, nil
	}
}

func (BigQueryDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Error ends up looking like something like this: Column already exists: _string at [1:39]
	return strings.Contains(err.Error(), "Column already exists")
}

func (BigQueryDialect) IsTableDoesNotExistErr(_ error) bool {
	return false
}

func (BigQueryDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, temporary bool, colSQLParts []string) string {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))

	if temporary {
		return fmt.Sprintf(
			`%s OPTIONS (expiration_timestamp = TIMESTAMP("%s"))`,
			query,
			BQExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)),
		)
	} else {
		return query
	}
}

func (BigQueryDialect) BuildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	return fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}

func (bd BigQueryDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	colName := sql.QuoteTableAliasColumn(tableAlias, column, bd)
	if column.KindDetails == typing.Struct {
		return fmt.Sprintf(`COALESCE(TO_JSON_STRING(%s) != '{"key":"%s"}', true)`,
			colName, constants.ToastUnavailableValuePlaceholder)
	}
	return fmt.Sprintf("COALESCE(%s != '%s', true)", colName, constants.ToastUnavailableValuePlaceholder)
}

func (bd BigQueryDialect) BuildDedupeTableQuery(tableID sql.TableIdentifier, primaryKeys []string) string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, bd)

	// BigQuery does not like DISTINCT for JSON columns, so we wrote this instead.
	// Error: Column foo of type JSON cannot be used in SELECT DISTINCT
	return fmt.Sprintf(`(SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1)`,
		tableID.FullyQualifiedName(),
		strings.Join(primaryKeysEscaped, ", "),
		strings.Join(primaryKeysEscaped, ", "),
	)
}

func (bd BigQueryDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, bd)

	orderColsToIterate := primaryKeysEscaped
	if topicConfig.IncludeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, bd.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, orderByCol := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s ASC", orderByCol))
	}

	var parts []string
	parts = append(parts,
		fmt.Sprintf(`CREATE OR REPLACE TABLE %s OPTIONS (expiration_timestamp = TIMESTAMP("%s")) AS (SELECT * FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2)`,
			stagingTableID.FullyQualifiedName(),
			BQExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)),
			tableID.FullyQualifiedName(),
			strings.Join(primaryKeysEscaped, ", "),
			strings.Join(orderByCols, ", "),
		),
	)

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		whereClauses = append(whereClauses, fmt.Sprintf("t1.%s = t2.%s", primaryKeyEscaped, primaryKeyEscaped))
	}

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_with_subquery
	parts = append(parts,
		fmt.Sprintf("DELETE FROM %s t1 WHERE EXISTS (SELECT * FROM %s t2 WHERE %s)",
			tableID.FullyQualifiedName(),
			stagingTableID.FullyQualifiedName(),
			strings.Join(whereClauses, " AND "),
		),
	)

	parts = append(parts, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableID.FullyQualifiedName(), stagingTableID.FullyQualifiedName()))
	return parts
}

func (bd BigQueryDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	idempotentKey string,
	primaryKeys []columns.Column,
	additionalEqualityStrings []string,
	cols []columns.Column,
	softDelete bool,
	_ bool,
) ([]string, error) {
	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// However, there may be edge cases where folks end up restoring deleted rows (which will contain the same PK).

	// We also need to do staged table's idempotency key is GTE target table's idempotency key
	// This is because Snowflake does not respect NS granularity.
	var idempotentClause string
	if idempotentKey != "" {
		idempotentClause = fmt.Sprintf("AND %s.%s >= %s.%s ", constants.StagingAlias, idempotentKey, constants.TargetAlias, idempotentKey)
	}

	var equalitySQLParts []string
	for _, primaryKey := range primaryKeys {
		equalitySQL := sql.BuildColumnComparison(primaryKey, constants.TargetAlias, constants.StagingAlias, sql.Equal, bd)

		if primaryKey.KindDetails.Kind == typing.Struct.Kind {
			// BigQuery requires special casting to compare two JSON objects.
			equalitySQL = fmt.Sprintf("TO_JSON_STRING(%s) = TO_JSON_STRING(%s)",
				sql.QuoteTableAliasColumn(constants.TargetAlias, primaryKey, bd),
				sql.QuoteTableAliasColumn(constants.StagingAlias, primaryKey, bd))
		}

		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}
	if len(additionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, additionalEqualityStrings...)
	}

	baseQuery := fmt.Sprintf(`
MERGE INTO %s %s USING %s AS %s ON %s`,
		tableID.FullyQualifiedName(), constants.TargetAlias, subQuery, constants.StagingAlias, strings.Join(equalitySQLParts, " AND "),
	)

	if softDelete {
		return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s);`,
			// WHEN MATCHED %sTHEN UPDATE SET %s
			idempotentClause, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, bd),
			// WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s)
			sql.QuotedDeleteColumnMarker(constants.StagingAlias, bd), strings.Join(sql.QuoteColumns(cols, bd), ","),
			// VALUES (%s);
			strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, bd), ","),
		)}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	cols, removed := columns.RemoveDeleteColumnMarker(cols)
	if !removed {
		return []string{}, errors.New("artie delete flag doesn't exist")
	}

	return []string{baseQuery + fmt.Sprintf(`
WHEN MATCHED AND %s THEN DELETE
WHEN MATCHED AND IFNULL(%s, false) = false %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s) VALUES (%s);`,
		// WHEN MATCHED AND %s THEN DELETE
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, bd),
		// WHEN MATCHED AND IFNULL(%s, false) = false %sTHEN UPDATE SET %s
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, bd), idempotentClause, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, bd),
		// WHEN NOT MATCHED AND IFNULL(%s, false) = false THEN INSERT (%s)
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, bd), strings.Join(sql.QuoteColumns(cols, bd), ","),
		// VALUES (%s);
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, bd), ","),
	)}, nil
}
