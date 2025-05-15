package converters

import (
	"cmp"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Converter interface {
	Convert(value any) (string, error)
}

type GetStringConverterOpts struct {
	TimestampTZLayoutOverride  string
	TimestampNTZLayoutOverride string
	UseNewStringMethod         bool
}

func GetStringConverter(kd typing.KindDetails, opts GetStringConverterOpts) (Converter, error) {
	switch kd.Kind {
	// Base types
	case typing.Boolean.Kind:
		return BooleanConverter{}, nil
	case typing.String.Kind:
		return StringConverter{
			useNewMethod: opts.UseNewStringMethod,
		}, nil
	// Time types
	case typing.Date.Kind:
		return DateConverter{}, nil
	case typing.Time.Kind:
		return TimeConverter{}, nil
	case typing.TimestampNTZ.Kind:
		return NewTimestampNTZConverter(opts.TimestampNTZLayoutOverride), nil
	case typing.TimestampTZ.Kind:
		return NewTimestampTZConverter(opts.TimestampTZLayoutOverride), nil
	// Array and struct types
	case typing.Array.Kind:
		return ArrayConverter{}, nil
	case typing.Struct.Kind:
		return StructConverter{}, nil
	// Numbers types
	case typing.EDecimal.Kind:
		return DecimalConverter{}, nil
	case typing.Integer.Kind:
		return IntegerConverter{}, nil
	case typing.Float.Kind:
		return FloatConverter{}, nil
	default:
		return nil, fmt.Errorf("unsupported type: %q", kd.Kind)
	}
}

type BooleanConverter struct{}

func (BooleanConverter) Convert(value any) (string, error) {
	switch castedValue := value.(type) {
	case bool:
		return fmt.Sprint(castedValue), nil
	default:
		// Try to cast the value into a string and see if we can parse it
		// If not, then return an error
		switch strings.ToLower(fmt.Sprint(value)) {
		case "0", "false":
			return "false", nil
		case "1", "true":
			return "true", nil
		default:
			return "", fmt.Errorf("failed to cast colVal as boolean, colVal: '%v', type: %T", value, value)
		}
	}
}

type StringConverter struct {
	useNewMethod bool
}

func (s StringConverter) Convert(value any) (string, error) {
	if s.useNewMethod {
		return s.ConvertNew(value)
	}

	return s.ConvertOld(value)
}

func (StringConverter) ConvertNew(value any) (string, error) {
	switch castedValue := value.(type) {
	case int, int8, int16, int32, int64:
		return IntegerConverter{}.Convert(castedValue)
	case float32, float64:
		return FloatConverter{}.Convert(castedValue)
	case bool:
		return BooleanConverter{}.Convert(castedValue)
	case string:
		return castedValue, nil
	case map[string]any:
		return StructConverter{}.Convert(castedValue)
	case time.Time:
		return TimestampTZConverter{}.Convert(castedValue)
	case *decimal.Decimal:
		return DecimalConverter{}.Convert(castedValue)
	default:
		if reflect.ValueOf(value).Kind() == reflect.Slice {
			return ArrayConverter{}.Convert(value)
		}

		return "", fmt.Errorf("unsupported value: %v, type: %T", value, value)
	}
}

func (StringConverter) ConvertOld(value any) (string, error) {
	// TODO Simplify this function
	isArray := reflect.ValueOf(value).Kind() == reflect.Slice
	_, isMap := value.(map[string]any)
	// If colVal is either an array or a JSON object, we should run JSON parse.
	if isMap || isArray {
		colValBytes, err := json.Marshal(value)
		if err != nil {
			return "", err
		}

		return string(colValBytes), nil
	}

	return stringutil.EscapeBackslashes(fmt.Sprint(value)), nil
}

type DateConverter struct{}

func (DateConverter) Convert(value any) (string, error) {
	_time, err := ext.ParseDateFromAny(value)
	if err != nil {
		return "", fmt.Errorf("failed to cast colVal as date, colVal: '%v', err: %w", value, err)
	}

	return _time.Format(time.DateOnly), nil
}

type TimeConverter struct{}

func (TimeConverter) Convert(value any) (string, error) {
	_time, err := ext.ParseTimeFromAny(value)
	if err != nil {
		return "", fmt.Errorf("failed to cast colVal as time, colVal: '%v', err: %w", value, err)
	}

	return _time.Format(ext.PostgresTimeFormatNoTZ), nil
}

func NewTimestampNTZConverter(layoutOverride string) TimestampNTZConverter {
	return TimestampNTZConverter{
		layoutOverride: layoutOverride,
	}
}

type TimestampNTZConverter struct {
	layoutOverride string
}

func (t TimestampNTZConverter) Convert(value any) (string, error) {
	_time, err := ext.ParseTimestampNTZFromAny(value)
	if err != nil {
		return "", fmt.Errorf("failed to cast colVal as timestampNTZ, colVal: '%v', err: %w", value, err)
	}

	return _time.Format(cmp.Or(t.layoutOverride, ext.RFC3339NoTZ)), nil
}

func NewTimestampTZConverter(layoutOverride string) TimestampTZConverter {
	return TimestampTZConverter{
		layoutOverride: layoutOverride,
	}
}

type TimestampTZConverter struct {
	layoutOverride string
}

func (t TimestampTZConverter) Convert(value any) (string, error) {
	_time, err := ext.ParseTimestampTZFromAny(value)
	if err != nil {
		return "", fmt.Errorf("failed to cast colVal as timestampTZ, colVal: '%v', err: %w", value, err)
	}

	return _time.Format(cmp.Or(t.layoutOverride, time.RFC3339Nano)), nil
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
	case *decimal.Decimal:
		return parsedVal.String(), nil
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
	case *decimal.Decimal:
		return parsedVal.String(), nil
	case string:
		return parsedVal, nil
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

type StructConverter struct{}

func (StructConverter) Convert(value any) (string, error) {
	if strings.Contains(fmt.Sprint(value), constants.ToastUnavailableValuePlaceholder) {
		return fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder), nil
	}

	switch castedValue := (value).(type) {
	case string:
		return fmt.Sprintf("%q", castedValue), nil
	default:
		colValBytes, err := json.Marshal(value)
		if err != nil {
			return "", err
		}

		return string(colValBytes), nil
	}
}
