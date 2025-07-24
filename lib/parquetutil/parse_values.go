package parquetutil

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/converters/primitives"
)

// ParseValueForArrow converts a raw value to Arrow-compatible format given column details and location
func ParseValueForArrow(value any, kindDetails typing.KindDetails) (any, error) {
	return kindDetails.ParseValueForArrow(value)
}

// ConvertValueForArrowBuilder converts a parsed value to the appropriate Arrow builder method call
func ConvertValueForArrowBuilder(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.StringBuilder:
		castedValue, err := typing.AssertType[string](value)
		if err != nil {
			return fmt.Errorf("failed to cast value to string: %w", err)
		}
		b.Append(castedValue)
	case *array.Int64Builder:
		castedValue, err := primitives.Int64Converter{}.Convert(value)
		if err != nil {
			return fmt.Errorf("failed to cast value to int64: %w", err)
		}
		b.Append(castedValue)
	case *array.BooleanBuilder:
		castedValue, err := primitives.BooleanConverter{}.Convert(value)
		if err != nil {
			return fmt.Errorf("failed to cast value to boolean: %w", err)
		}
		b.Append(castedValue)
	case *array.Float32Builder:
		if floatVal, ok := value.(float32); ok {
			b.Append(floatVal)
		} else if floatVal, ok := value.(float64); ok {
			b.Append(float32(floatVal))
		} else {
			return fmt.Errorf("expected float32 value, got %T", value)
		}
	case *array.Time32Builder:
		if timeVal, ok := value.(int32); ok {
			b.Append(arrow.Time32(timeVal))
		} else {
			return fmt.Errorf("expected int32 time value, got %T", value)
		}
	case *array.Date32Builder:
		if dateVal, ok := value.(int32); ok {
			b.Append(arrow.Date32(dateVal))
		} else {
			return fmt.Errorf("expected int32 date value, got %T", value)
		}
	case *array.TimestampBuilder:
		if tsVal, ok := value.(int64); ok {
			b.Append(arrow.Timestamp(tsVal))
		} else {
			return fmt.Errorf("expected int64 timestamp value, got %T", value)
		}
	case *array.Decimal128Builder:
		if decVal, ok := value.(decimal128.Num); ok {
			b.Append(decVal)
		} else {
			return fmt.Errorf("expected decimal128.Num value, got %T", value)
		}
	case *array.ListBuilder:
		// For now, handle arrays as strings
		b.Append(true) // Start list
		valueBuilder := b.ValueBuilder().(*array.StringBuilder)
		valueBuilder.Append(fmt.Sprintf("%v", value))
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}
