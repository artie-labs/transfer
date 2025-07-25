package parquetutil

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/artie-labs/transfer/lib/typing"
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
		castedValue, err := typing.AssertType[int64](value)
		if err != nil {
			return fmt.Errorf("failed to cast value to int64: %w", err)
		}
		b.Append(castedValue)
	case *array.BooleanBuilder:
		castedValue, err := typing.AssertType[bool](value)
		if err != nil {
			return fmt.Errorf("failed to cast value to boolean: %w", err)
		}
		b.Append(castedValue)
	case *array.Float32Builder:
		castedValue, err := typing.AssertType[float32](value)
		if err != nil {
			return fmt.Errorf("failed to cast value to float32: %w", err)
		}
		b.Append(castedValue)
	case *array.Time32Builder:
		castedValue, err := typing.AssertType[int32](value)
		if err != nil {
			return fmt.Errorf("failed to cast value to int32: %w", err)
		}
		b.Append(arrow.Time32(castedValue))
	case *array.Date32Builder:
		castedValue, err := typing.AssertType[int32](value)
		if err != nil {
			return fmt.Errorf("failed to cast value to int32: %w", err)
		}
		b.Append(arrow.Date32(castedValue))
	case *array.TimestampBuilder:
		castedValue, err := typing.AssertType[int64](value)
		if err != nil {
			return fmt.Errorf("failed to cast value to int64: %w", err)
		}
		b.Append(arrow.Timestamp(castedValue))
	case *array.Decimal128Builder:
		castedValue, err := typing.AssertType[decimal128.Num](value)
		if err != nil {
			return fmt.Errorf("failed to cast value to decimal128.Num: %w", err)
		}
		b.Append(castedValue)
	case *array.ListBuilder:
		castedValue, err := typing.AssertType[[]string](value)
		if err != nil {
			return fmt.Errorf("failed to cast value to []string: %w", err)
		}

		b.Append(true)
		valueBuilder, ok := b.ValueBuilder().(*array.StringBuilder)
		if !ok {
			return fmt.Errorf("failed to cast value builder to array.StringBuilder")
		}

		for _, item := range castedValue {
			valueBuilder.Append(item)
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}
