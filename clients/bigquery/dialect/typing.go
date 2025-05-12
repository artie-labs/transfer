package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func (BigQueryDialect) DataTypeForKind(kindDetails typing.KindDetails, _ bool, settings config.SharedDestinationColumnSettings) string {
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
	case typing.Date.Kind:
		return "date"
	case typing.Time.Kind:
		return "time"
	case typing.TimestampNTZ.Kind:
		return "datetime"
	case typing.TimestampTZ.Kind:
		// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type
		// We should be using TIMESTAMP since it's an absolute point in time.
		return "timestamp"
	case typing.EDecimal.Kind:
		// [kindDetails.ExtendedDecimalDetails] may be nil if the target data type is a variable numeric or bignumeric.
		if kindDetails.ExtendedDecimalDetails == nil {
			if settings.BigNumericForVariableNumeric() {
				return "bignumeric"
			} else {
				return "numeric"
			}
		}

		return kindDetails.ExtendedDecimalDetails.BigQueryKind(settings.BigNumericForVariableNumeric())
	}

	return kindDetails.Kind
}

func (BigQueryDialect) KindForDataType(rawBqType string, _ string) (typing.KindDetails, error) {
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
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
	case "numeric":
		if len(parameters) == 0 {
			// BigQuery [NUMERIC] type will default to NUMERIC(38, 9)
			return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(38, 9)), nil
		}
		return typing.ParseNumeric(parameters)
	case "bignumeric":
		if len(parameters) == 0 {
			fmt.Println("bignumeric default")
			// BigQuery [BIGNUMERIC] type will default to BIGNUMERIC(76, 38)
			return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(76, 38)), nil
		}

		fmt.Println("bignumeric", parameters)
		return typing.ParseNumeric(parameters)
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
	case "timestamp":
		return typing.TimestampTZ, nil
	case "datetime":
		return typing.TimestampNTZ, nil
	case "time":
		return typing.Time, nil
	case "date":
		return typing.Date, nil
	default:
		return typing.Invalid, fmt.Errorf("unsupported data type: %q", rawBqType)
	}
}
