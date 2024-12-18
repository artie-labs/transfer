package converters

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type StringConverter interface {
	Convert(value any) (string, error)
}

func GetStringConverter(kd typing.KindDetails) (StringConverter, error) {
	switch kd.Kind {
	// Time
	case typing.Date.Kind:
		return DateConverter{}, nil
	case typing.Time.Kind:
		return TimeConverter{}, nil
	case typing.TimestampNTZ.Kind:
		return TimestampNTZConverter{}, nil
	case typing.TimestampTZ.Kind:
		return TimestampTZConverter{}, nil
	case typing.Array.Kind:
		return ArrayConverter{}, nil
	// Numbers
	case typing.EDecimal.Kind:
		return DecimalConverter{}, nil
	case typing.Integer.Kind:
		return IntegerConverter{}, nil
	case typing.Float.Kind:
		return FloatConverter{}, nil
	}

	// TODO: Return an error when all the types are implemented.
	return nil, nil
}

type DateConverter struct{}

func (DateConverter) Convert(value any) (string, error) {
	_time, err := ext.ParseDateFromAny(value)
	if err != nil {
		return "", fmt.Errorf("failed to cast colVal as date, colVal: '%v', err: %w", value, err)
	}

	return _time.Format(ext.PostgresDateFormat), nil
}

type TimeConverter struct{}

func (TimeConverter) Convert(value any) (string, error) {
	_time, err := ext.ParseTimeFromAny(value)
	if err != nil {
		return "", fmt.Errorf("failed to cast colVal as time, colVal: '%v', err: %w", value, err)
	}

	return _time.Format(ext.PostgresTimeFormatNoTZ), nil
}

type TimestampNTZConverter struct{}

func (TimestampNTZConverter) Convert(value any) (string, error) {
	_time, err := ext.ParseTimestampNTZFromAny(value)
	if err != nil {
		return "", fmt.Errorf("failed to cast colVal as timestampNTZ, colVal: '%v', err: %w", value, err)
	}

	return _time.Format(ext.RFC3339NoTZ), nil
}

type TimestampTZConverter struct{}

func (TimestampTZConverter) Convert(value any) (string, error) {
	_time, err := ext.ParseTimestampTZFromAny(value)
	if err != nil {
		return "", fmt.Errorf("failed to cast colVal as timestampTZ, colVal: '%v', err: %w", value, err)
	}

	return _time.Format(time.RFC3339Nano), nil
}

type ArrayConverter struct{}

func (ArrayConverter) Convert(value any) (string, error) {
	// If the column value is TOASTED, we should return an array with the TOASTED placeholder
	// We're doing this to make sure that the value matches the schema.
	if stringValue, ok := value.(string); ok {
		if stringValue == constants.ToastUnavailableValuePlaceholder {
			return fmt.Sprintf(`["%s"]`, constants.ToastUnavailableValuePlaceholder), nil
		}
	}

	colValBytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	return string(colValBytes), nil
}

type IntegerConverter struct{}

func (IntegerConverter) Convert(value any) (string, error) {
	switch parsedVal := value.(type) {
	case float32:
		return Float32ToString(parsedVal), nil
	case float64:
		return Float64ToString(parsedVal), nil
	case bool:
		return fmt.Sprint(BooleanToBit(parsedVal)), nil
	case int, int8, int16, int32, int64:
		return fmt.Sprint(parsedVal), nil
	default:
		return "", fmt.Errorf("unexpected value: '%v', type: %T", value, value)
	}
}

type FloatConverter struct{}

func (FloatConverter) Convert(value any) (string, error) {
	switch parsedVal := value.(type) {
	case float32:
		return Float32ToString(parsedVal), nil
	case float64:
		return Float64ToString(parsedVal), nil
	case int, int8, int16, int32, int64:
		return fmt.Sprint(parsedVal), nil
	default:
		return "", fmt.Errorf("unexpected value: '%v', type: %T", value, value)
	}
}

type DecimalConverter struct{}

func (DecimalConverter) Convert(value any) (string, error) {
	switch castedColVal := value.(type) {
	case float32:
		return Float32ToString(castedColVal), nil
	case float64:
		return Float64ToString(castedColVal), nil
	case int, int8, int16, int32, int64:
		return fmt.Sprint(castedColVal), nil
	case string:
		return castedColVal, nil
	case *decimal.Decimal:
		return castedColVal.String(), nil
	default:
		return "", fmt.Errorf("unexpected value: '%v' type: %T", value, value)
	}
}
