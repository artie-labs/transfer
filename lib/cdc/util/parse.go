package util

import (
	"context"
	"fmt"
	"strconv"

	"github.com/artie-labs/transfer/lib/jsonutil"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/logger"
)

// ParseField returns a `parsedValue` as type interface{}
func parseField(ctx context.Context, field debezium.Field, value interface{}) interface{} {
	if value == nil {
		return nil
	}

	// Check if the field is an integer and requires us to cast it as such.
	if field.IsInteger() {
		valFloat, isOk := value.(float64)
		if isOk {
			return int(valFloat)
		}
	}

	if valid, supportedType := debezium.RequiresSpecialTypeCasting(field.DebeziumType); valid {
		switch debezium.SupportedDebeziumType(field.DebeziumType) {
		case debezium.JSON:
			valString, err := jsonutil.SanitizePayload(value)
			if err == nil {
				return valString
			}
		case debezium.KafkaDecimalType:
			decimalVal, err := field.DecodeDecimal(fmt.Sprint(value))
			if err == nil {
				return decimalVal
			} else {
				logger.FromContext(ctx).WithFields(map[string]interface{}{
					"err":           err,
					"supportedType": supportedType,
					"val":           value,
				}).Debug("skipped casting dbz type due to an error")
			}
		case debezium.KafkaVariableNumericType:
			variableNumericVal, err := field.DecodeDebeziumVariableDecimal(value)
			if err == nil {
				return variableNumericVal
			} else {
				logger.FromContext(ctx).WithFields(map[string]interface{}{
					"err":           err,
					"supportedType": supportedType,
					"val":           value,
				}).Debug("skipped casting dbz type due to an error")
			}
		default:
			// Need to cast this as a FLOAT first because the number may come out in scientific notation
			// ParseFloat is apt to handle it, and ParseInt is not, see: https://github.com/golang/go/issues/19288
			floatVal, castErr := strconv.ParseFloat(fmt.Sprint(value), 64)
			if castErr == nil {
				extendedTime, err := debezium.FromDebeziumTypeToTime(supportedType, int64(floatVal))
				if err == nil {
					return extendedTime
				} else {
					logger.FromContext(ctx).WithFields(map[string]interface{}{
						"err":           err,
						"supportedType": supportedType,
						"val":           value,
					}).Debug("skipped casting dbz type due to an error")
				}
			} else {
				logger.FromContext(ctx).WithFields(map[string]interface{}{
					"err":           castErr,
					"supportedType": supportedType,
					"val":           value,
				}).Debug("skipped casting because we failed to parse the float")
			}
		}
	}

	return value
}
