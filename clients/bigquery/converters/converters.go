package converters

import (
	"fmt"
	"strconv"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/values"
)

type StringConverter struct {
	kd typing.KindDetails
}

func NewStringConverter(kd typing.KindDetails) StringConverter {
	return StringConverter{kd: kd}
}

func (s StringConverter) Convert(value any) (any, error) {
	return values.ToString(value, s.kd)
}

type Int64Converter struct{}

func (Int64Converter) Convert(value any) (any, error) {
	switch castedValue := value.(type) {
	case int:
		return int64(castedValue), nil
	case int32:
		return int64(castedValue), nil
	case int64:
		return castedValue, nil
	case float32:
		return int64(castedValue), nil
	case float64:
		return int64(castedValue), nil
	default:
		return nil, fmt.Errorf("expected int/int32/int64/float32/float64 received %T with value %v", value, value)
	}
}

type BooleanConverter struct{}

func (BooleanConverter) Convert(value any) (any, error) {
	switch castedValue := value.(type) {
	case bool:
		return castedValue, nil
	case string:
		val, err := strconv.ParseBool(castedValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse bool %q: %w", castedValue, err)
		}

		return val, nil
	default:
		return nil, fmt.Errorf("expected bool received %T with value %v", value, value)
	}
}

type Float64Converter struct{}

func (Float64Converter) Convert(value any) (any, error) {
	switch castedVal := value.(type) {
	case float32:
		return float64(castedVal), nil
	case float64:
		return castedVal, nil
	case int32:
		return float64(castedVal), nil
	case int64:
		return float64(castedVal), nil
	case *decimal.Decimal:
		floatValue, err := castedVal.Value().Float64()
		if err != nil {
			return nil, fmt.Errorf("failed to convert decimal to float64: %w", err)
		}

		return floatValue, nil
	case string:
		floatValue, err := strconv.ParseFloat(castedVal, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse string %q to float64: %w", castedVal, err)
		}

		return floatValue, nil
	default:
		return nil, fmt.Errorf("failed to run float64 converter, unexpected type %T with value %v", value, value)
	}
}
