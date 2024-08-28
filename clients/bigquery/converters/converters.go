package converters

import (
	"fmt"
	"strconv"

	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type StringConverter struct{}

func (StringConverter) Convert(value any) (any, error) {
	switch castedValue := value.(type) {
	case string:
		return castedValue, nil
	case *decimal.Decimal:
		return castedValue.String(), nil
	case bool:
		return fmt.Sprint(castedValue), nil
	case *ext.ExtendedTime:
		return castedValue.String(""), nil
	default:
		return nil, fmt.Errorf("expected string/*decimal.Decimal/bool received %T with value %v", value, value)
	}
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
	default:
		return nil, fmt.Errorf("expected int/int32/int64 received %T with value %v", value, value)
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
			return nil, fmt.Errorf("failed to parse string to float64: %w", err)
		}

		return floatValue, nil
	default:
		return nil, fmt.Errorf("expected float32/float64/int32/int64/*decimal.Decimal/string received %T with value %v", value, value)
	}
}
