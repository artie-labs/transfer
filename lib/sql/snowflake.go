package sql

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type SnowflakeDialect struct{}

func (sd SnowflakeDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ToUpper(identifier))
}

func (SnowflakeDialect) EscapeStruct(value string) string {
	return QuoteLiteral(value)
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
	snowflakeType = strings.ToLower(snowflakeType)

	// We need to strip away the variable
	// For example, a Column can look like: TEXT, or Number(38, 0) or VARCHAR(255).
	// We need to strip out all the content from ( ... )
	if len(snowflakeType) == 0 {
		return typing.Invalid, nil
	}

	dataType, _, err := ParseDataTypeDefinition(snowflakeType)
	if err != nil {
		return typing.Invalid, err
	}

	// Geography, geometry date, time, varbinary, binary are currently not supported.
	switch strings.TrimSpace(dataType) {
	case "number":
		return typing.ParseNumeric("number", snowflakeType), nil
	case "numeric":
		return typing.ParseNumeric(typing.DefaultPrefix, snowflakeType), nil
	case "decimal":
		return typing.EDecimal, nil
	case "float", "float4",
		"float8", "double", "double precision", "real":
		return typing.Float, nil
	case "int", "integer", "bigint", "smallint", "tinyint", "byteint":
		return typing.Integer, nil
	case "varchar", "char", "character", "string", "text":
		return typing.String, nil
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

func (SnowflakeDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	// TEMPORARY Table syntax - https://docs.snowflake.com/en/sql-reference/sql/create-table
	// PURGE syntax - https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#purging-files-after-loading
	// FIELD_OPTIONALLY_ENCLOSED_BY - is needed because CSV will try to escape any values that have `"`
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE)`,
		fqTableName, strings.Join(colSQLParts, ","))
}

func (SnowflakeDialect) BuildProcessToastStructColExpression(colName string) string {
	// TODO: Change this to Snowflake and error out if the destKind isn't supported so we're explicit.
	return fmt.Sprintf("CASE WHEN COALESCE(cc.%s != {'key': '%s'}, true) THEN cc.%s ELSE c.%s END",
		colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
}
