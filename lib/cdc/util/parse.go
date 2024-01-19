package util

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/artie-labs/transfer/lib/jsonutil"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/debezium"
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
			// TODO: What about arrays?
			valString, err := jsonutil.SanitizePayload(value)
			if err == nil {
				return valString
			}
		case debezium.KafkaDecimalType:
			decimalVal, err := field.DecodeDecimal(fmt.Sprint(value))
			if err == nil {
				return decimalVal
			} else {
				slog.Debug("skipped casting dbz type due to an error",
					slog.Any("err", err),
					slog.Any("supportedType", supportedType),
					slog.Any("val", value),
				)
			}
		case debezium.KafkaVariableNumericType:
			variableNumericVal, err := field.DecodeDebeziumVariableDecimal(value)
			if err == nil {
				return variableNumericVal
			} else {
				slog.Debug("skipped casting dbz type due to an error",
					slog.Any("err", err),
					slog.Any("supportedType", supportedType),
					slog.Any("val", value),
				)
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
					if ext.IsInvalidErr(err) {
						slog.Info("extTime is not valid, so returning nil here instead",
							slog.Any("err", err),
							slog.Any("supportedType", supportedType),
							slog.Any("val", value),
						)
						return nil
					}

					slog.Debug("skipped casting dbz type due to an error",
						slog.Any("err", err),
						slog.Any("supportedType", supportedType),
						slog.Any("val", value),
					)
				}
			} else {
				slog.Debug("skipped casting because we failed to parse the float",
					slog.Any("err", castErr),
					slog.Any("supportedType", supportedType),
					slog.Any("val", value),
				)
			}
		}
	}

	return value
}
