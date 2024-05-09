package sql

import (
	"fmt"
	"strings"

	"strconv"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type RedshiftDialect struct{}

func (rd RedshiftDialect) QuoteIdentifier(identifier string) string {
	// Preserve the existing behavior of Redshift identifiers being lowercased due to not being quoted.
	return fmt.Sprintf(`"%s"`, strings.ToLower(identifier))
}

func (RedshiftDialect) EscapeStruct(value string) string {
	return fmt.Sprintf("JSON_PARSE(%s)", QuoteLiteral(value))
}

func (RedshiftDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	switch kd.Kind {
	case typing.Integer.Kind:
		return "INT8"
	case typing.Struct.Kind:
		return "SUPER"
	case typing.Array.Kind:
		return "VARCHAR(MAX)"
	case typing.String.Kind:
		if kd.OptionalStringPrecision != nil {
			return fmt.Sprintf("VARCHAR(%d)", *kd.OptionalStringPrecision)
		}
		return "VARCHAR(MAX)"
	case typing.Boolean.Kind:
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
	if strings.HasPrefix(rawType, "numeric") {
		return typing.ParseNumeric(typing.DefaultPrefix, rawType), nil
	}
	if strings.Contains(rawType, "character varying") {
		var strPrecision *int
		precision, err := strconv.Atoi(stringPrecision)
		if err == nil {
			strPrecision = &precision
		}
		return typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: strPrecision}, nil
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

func (RedshiftDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", fqTableName, strings.Join(colSQLParts, ","))
}

func (RedshiftDialect) BuildProcessToastStructColExpression(colName string) string {
	return fmt.Sprintf(`CASE WHEN COALESCE(cc.%s != JSON_PARSE('{"key":"%s"}'), true) THEN cc.%s ELSE c.%s END`,
		colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
}
