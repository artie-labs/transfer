package typing

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/apache/arrow/go/v17/arrow/decimal256"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/typing/converters/primitives"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func millisecondsAfterMidnight(t time.Time) int32 {
	year, month, day := t.Date()
	midnight := time.Date(year, month, day, 0, 0, 0, 0, t.Location())
	return int32(t.Sub(midnight).Milliseconds())
}

var kindToArrowType = map[string]arrow.DataType{
	String.Kind:  arrow.BinaryTypes.String,
	Boolean.Kind: arrow.FixedWidthTypes.Boolean,
	// Number data types:
	Integer.Kind: arrow.PrimitiveTypes.Int64,
	Float.Kind:   arrow.PrimitiveTypes.Float32,
	// Date and time data types:
	TimeKindDetails.Kind: arrow.FixedWidthTypes.Time32ms,
	Date.Kind:            arrow.FixedWidthTypes.Date32,
}

// ToArrowType converts a KindDetails to the corresponding Arrow data type
func (kd KindDetails) ToArrowType() (arrow.DataType, error) {
	if arrowType, ok := kindToArrowType[kd.Kind]; ok {
		return arrowType, nil
	}

	switch kd.Kind {
	case EDecimal.Kind:
		if kd.ExtendedDecimalDetails == nil {
			return nil, fmt.Errorf("extended decimal details are not set")
		}

		if kd.ExtendedDecimalDetails != nil {
			precision := kd.ExtendedDecimalDetails.Precision()
			if precision == decimal.PrecisionNotSpecified {
				return arrow.BinaryTypes.String, nil
			}

			if precision <= 38 {
				return &arrow.Decimal128Type{Precision: precision, Scale: kd.ExtendedDecimalDetails.Scale()}, nil
			} else if precision <= 76 {
				return &arrow.Decimal256Type{Precision: precision, Scale: kd.ExtendedDecimalDetails.Scale()}, nil
			}
		}
		// Default decimal or unsupported precision - use string
		return arrow.BinaryTypes.String, nil
	case Struct.Kind:
		// For struct types, we'll use string representation
		return arrow.BinaryTypes.String, nil
	case Array.Kind:
		// For arrays, we need to determine the element type
		// For now, default to list of strings
		return arrow.ListOf(arrow.BinaryTypes.String), nil
	case TimestampTZ.Kind:
		return arrow.FixedWidthTypes.Timestamp_ms, nil
	case TimestampNTZ.Kind:
		return arrow.FixedWidthTypes.Timestamp_ms, nil
	default:
		return nil, fmt.Errorf("unsupported kind %q", kd.Kind)
	}
}

// ParseValueForArrow converts a value to the appropriate Arrow-compatible type
func (kd KindDetails) ParseValueForArrow(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch kd.Kind {
	case String.Kind:
		return value, nil
	case Struct.Kind:
		out, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal struct: %w", err)
		}
		return string(out), nil
	case Array.Kind:
		arrayString, err := array.InterfaceToArrayString(value, true)
		if err != nil {
			return nil, err
		}

		if len(arrayString) == 0 {
			return nil, nil
		}

		return arrayString, nil
	case Integer.Kind:
		return primitives.Int64Converter{}.Convert(value)
	case Boolean.Kind:
		return primitives.BooleanConverter{}.Convert(value)
	case Float.Kind:
		return primitives.Float32Converter{}.Convert(value)
	case EDecimal.Kind:
		if kd.ExtendedDecimalDetails == nil {
			return nil, fmt.Errorf("extended decimal details are not set")
		}

		castedValue, err := AssertType[*decimal.Decimal](value)
		if err != nil {
			return nil, fmt.Errorf("failed to cast value to decimal.Decimal: %w", err)
		}

		precision := kd.ExtendedDecimalDetails.Precision()
		if precision == decimal.PrecisionNotSpecified {
			// Precision is not specified, so we'll default to a string.
			return castedValue.String(), nil
		}

		if precision <= 38 {
			return decimal128.FromString(castedValue.String(), precision, kd.ExtendedDecimalDetails.Scale())
		} else if precision <= 76 {
			return decimal256.FromString(castedValue.String(), precision, kd.ExtendedDecimalDetails.Scale())
		}

		return castedValue.String(), nil
	case TimeKindDetails.Kind:
		_time, err := ParseTimeFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to cast value to time: %w", err)
		}

		// TIME with unit MILLIS is used for millisecond precision. It must annotate an int32 that stores the number of milliseconds after midnight.
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time-millis
		return millisecondsAfterMidnight(_time), nil
	case Date.Kind:
		_time, err := ParseDateFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to cast value to date: %w", err)
		}

		// Days since epoch
		return int32(_time.UnixMilli() / (24 * time.Hour.Milliseconds())), nil
	case TimestampTZ.Kind:
		_time, err := ParseTimestampTZFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to cast value to timestamp: %w", err)
		}

		return _time.UnixMilli(), nil
	case TimestampNTZ.Kind:
		_time, err := ParseTimestampNTZFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to cast value to timestamp: %w", err)
		}

		return _time.UnixMilli(), nil
	default:
		return nil, fmt.Errorf("unsupported kind: %q with value type %T", kd.Kind, value)
	}
}
