package converters

import (
	"fmt"
	"strconv"

	"github.com/artie-labs/transfer/lib/typing/decimal"
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
	default:
		return nil, fmt.Errorf("expected string/*decimal.Decimal/bool received %T with value %v", value, value)
	}
}

type IntegerConverter struct{}

func (IntegerConverter) Convert(value any) (any, error) {
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
