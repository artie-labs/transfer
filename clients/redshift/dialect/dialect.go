package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type RedshiftDialect struct{}

func (rd RedshiftDialect) QuoteIdentifier(identifier string) string {
	// Preserve the existing behavior of Redshift identifiers being lowercased due to not being quoted.
	return fmt.Sprintf(`"%s"`, strings.ToLower(identifier))
}

func (RedshiftDialect) EscapeStruct(value string) string {
	return fmt.Sprintf("JSON_PARSE(%s)", sql.QuoteLiteral(value))
}

func (RedshiftDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	switch kd.Kind {
	case typing.Integer.Kind:
		// int4 is 2^31, whereas int8 is 2^63.
		// we're using a larger data type to not have an integer overflow.
		return "INT8"
	case typing.Struct.Kind:
		return "SUPER"
	case typing.Array.Kind:
		// Redshift does not have a built-in JSON type (which means we'll cast STRUCT and ARRAY kinds as TEXT).
		// As a result, Artie will store this in JSON string and customers will need to extract this data out via SQL.
		// Columns that are automatically created by Artie are created as VARCHAR(MAX).
		// Rationale: https://github.com/artie-labs/transfer/pull/173
		return "VARCHAR(MAX)"
	case typing.String.Kind:
		if kd.OptionalStringPrecision != nil {
			return fmt.Sprintf("VARCHAR(%d)", *kd.OptionalStringPrecision)
		}

		return "VARCHAR(MAX)"
	case typing.Boolean.Kind:
		// We need to append `NULL` to let Redshift know that NULL is an acceptable data type.
		return "BOOLEAN NULL"
	case typing.ETime.Kind:
		switch kd.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			return "timestamp with time zone"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	case typing.EDecimal.Kind:
		return kd.ExtendedDecimalDetails.RedshiftKind()
	}

	return kd.Kind
}

func (RedshiftDialect) KindForDataType(rawType string, stringPrecision string) (typing.KindDetails, error) {
	rawType = strings.ToLower(rawType)
	// TODO: Check if there are any missing Redshift data types.
	if strings.HasPrefix(rawType, "numeric") {
		_, parameters, err := sql.ParseDataTypeDefinition(rawType)
		if err != nil {
			return typing.Invalid, err
		}
		return typing.ParseNumeric(parameters), nil
	}

	if strings.Contains(rawType, "character varying") {
		var strPrecision *int
		precision, err := strconv.Atoi(stringPrecision)
		if err == nil {
			strPrecision = &precision
		}

		return typing.KindDetails{
			Kind:                    typing.String.Kind,
			OptionalStringPrecision: strPrecision,
		}, nil
	}

	switch rawType {
	case "super":
		return typing.Struct, nil
	case "smallint", "integer", "bigint":
		return typing.Integer, nil
	case "double precision":
		return typing.Float, nil
	case "timestamp with time zone", "timestamp without time zone":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType), nil
	case "time without time zone":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType), nil
	case "date":
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType), nil
	case "boolean":
		return typing.Boolean, nil
	}

	return typing.Invalid, nil
}

func (RedshiftDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Redshift's error: ERROR: column "foo" of relation "statement" already exists
	return strings.Contains(err.Error(), "already exists")
}

func (RedshiftDialect) IsTableDoesNotExistErr(err error) bool {
	return false
}

func (RedshiftDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Redshift uses the same syntax for temporary and permanant tables.
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (RedshiftDialect) BuildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	return fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}

func (RedshiftDialect) BuildProcessToastColExpression(colName string) string {
	return fmt.Sprintf("CASE WHEN COALESCE(cc.%s != '%s', true) THEN cc.%s ELSE c.%s END",
		colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
}

func (RedshiftDialect) BuildProcessToastStructColExpression(colName string) string {
	return fmt.Sprintf(`CASE WHEN COALESCE(cc.%s != JSON_PARSE('{"key":"%s"}'), true) THEN cc.%s ELSE c.%s END`,
		colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
}

func (rd RedshiftDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, rd)

	orderColsToIterate := primaryKeysEscaped
	if topicConfig.IncludeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, rd.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, orderByCol := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s ASC", orderByCol))
	}

	var parts []string
	parts = append(parts,
		// It looks funny, but we do need a WHERE clause to make the query valid.
		fmt.Sprintf("CREATE TEMPORARY TABLE %s AS (SELECT * FROM %s WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2)",
			// Temporary tables may not specify a schema name
			stagingTableID.EscapedTable(),
			tableID.FullyQualifiedName(),
			strings.Join(primaryKeysEscaped, ", "),
			strings.Join(orderByCols, ", "),
		),
	)

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		// Redshift does not support table aliasing for deletes.
		whereClauses = append(whereClauses, fmt.Sprintf("%s.%s = t2.%s", tableID.EscapedTable(), primaryKeyEscaped, primaryKeyEscaped))
	}

	// Delete duplicates in the main table based on matches with the staging table
	parts = append(parts,
		fmt.Sprintf("DELETE FROM %s USING %s t2 WHERE %s",
			tableID.FullyQualifiedName(),
			stagingTableID.EscapedTable(),
			strings.Join(whereClauses, " AND "),
		),
	)

	// Insert deduplicated data back into the main table from the staging table
	parts = append(parts,
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s",
			tableID.FullyQualifiedName(),
			stagingTableID.EscapedTable(),
		),
	)

	return parts
}

func (rd RedshiftDialect) BuildMergeDeleteQuery(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column) string {
	return fmt.Sprintf(`DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s AS cc WHERE cc.%s = true);`,
		// DELETE from table where (pk_1, pk_2)
		tableID.FullyQualifiedName(), strings.Join(columns.QuoteColumns(primaryKeys, rd), ","),
		// IN (cc.pk_1, cc.pk_2) FROM staging
		array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
			Vals:      columns.QuoteColumns(primaryKeys, rd),
			Separator: ",",
			Prefix:    "cc.",
		}), subQuery, rd.QuoteIdentifier(constants.DeleteColumnMarker),
	)
}
