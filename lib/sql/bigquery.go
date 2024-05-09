package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

const BQStreamingTimeFormat = "15:04:05"

const bqLayout = "2006-01-02 15:04:05 MST"

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
	return "JSON" + QuoteLiteral(value)
}

func (BigQueryDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	switch kd.Kind {
	case typing.Float.Kind:
		return "float64"
	case typing.Array.Kind:
		return "array<string>"
	case typing.Struct.Kind:
		return "json"
	case typing.ETime.Kind:
		switch kd.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			return "timestamp"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	case typing.EDecimal.Kind:
		return kd.ExtendedDecimalDetails.BigQueryKind()
	}
	return kd.Kind
}

func (BigQueryDialect) KindForDataType(_type string, _ string) (typing.KindDetails, error) {
	var rawBqType string = _type
	rawBqType = strings.ToLower(rawBqType)
	bqType := rawBqType
	if len(bqType) == 0 {
		return typing.Invalid, nil
	}
	idxStop := len(bqType)
	if idx := strings.Index(bqType, "("); idx > 0 {
		idxStop = idx
	}
	bqType = bqType[:idxStop]
	idxStop = len(bqType)
	if idx := strings.Index(bqType, "<"); idx > 0 {
		idxStop = idx
	}
	switch strings.TrimSpace(bqType[:idxStop]) {
	case "numeric":
		if rawBqType == "numeric" || rawBqType == "bignumeric" {
			return typing.EDecimal, nil
		}
		return typing.ParseNumeric(typing.DefaultPrefix, rawBqType), nil
	case "bignumeric":
		if rawBqType == "bignumeric" {
			return typing.EDecimal, nil
		}
		return typing.ParseNumeric("bignumeric", rawBqType), nil
	case "decimal", "float", "float64", "bigdecimal":
		return typing.Float, nil
	case "int", "integer", "int64":
		return typing.Integer, nil
	case "varchar", "string":
		return typing.String, nil
	case "bool", "boolean":
		return typing.Boolean, nil
	case "struct", "json", "record":
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

func (BigQueryDialect) BuildCreateTempTableQuery(fqTableName string, colSQLParts []string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) OPTIONS (expiration_timestamp = TIMESTAMP("%s"))`,
		fqTableName, strings.Join(colSQLParts, ","), BQExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)))
}

func (BigQueryDialect) BuildProcessToastStructColExpression(colName string) string {
	return fmt.Sprintf(`CASE WHEN COALESCE(TO_JSON_STRING(cc.%s) != '{"key":"%s"}', true) THEN cc.%s ELSE c.%s END`,
		colName, constants.ToastUnavailableValuePlaceholder,
		colName, colName)
}
