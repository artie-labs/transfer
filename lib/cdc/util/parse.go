package util

import (
	"fmt"
	"strconv"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/jsonutil"
)

func parseField(field debezium.Field, value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	// Check if the field is an integer and requires us to cast it as such.
	if field.IsInteger() {
		valFloat, isOk := value.(float64)
		if !isOk {
			return nil, fmt.Errorf("failed to cast value to float64")
		}

		return int(valFloat), nil
	}

	if valid, supportedType := debezium.RequiresSpecialTypeCasting(field.DebeziumType); valid {
		switch debezium.SupportedDebeziumType(field.DebeziumType) {
		case debezium.JSON:
			return jsonutil.SanitizePayload(value)
		case debezium.GeometryType, debezium.GeographyType:
			return parseGeometry(value)
		case debezium.GeometryPointType:
			return parseGeometryPoint(value)
		case debezium.KafkaDecimalType:
			bytes, err := debezium.ToBytes(value)
			if err != nil {
				return nil, err
			}
			return field.DecodeDecimal(bytes)
		case debezium.KafkaVariableNumericType:
			return field.DecodeDebeziumVariableDecimal(value)
		default:
			// Need to cast this as a FLOAT first because the number may come out in scientific notation
			// ParseFloat is apt to handle it, and ParseInt is not, see: https://github.com/golang/go/issues/19288
			floatVal, castErr := strconv.ParseFloat(fmt.Sprint(value), 64)
			if castErr != nil {
				return nil, castErr
			}

			return debezium.FromDebeziumTypeToTime(supportedType, int64(floatVal))
		}
	}

	return value, nil
}
