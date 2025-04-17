package converters

import (
	"encoding/base64"
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/jsonutil"
	"github.com/artie-labs/transfer/lib/typing"
)

type JSON struct{}

func (JSON) Convert(value any) (any, error) {
	valueString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	if valueString == constants.ToastUnavailableValuePlaceholder {
		return value, nil
	}

	return jsonutil.SanitizePayload(valueString)
}

func (JSON) ToKindDetails() typing.KindDetails {
	return typing.Struct
}

type Int64Passthrough struct{}

func (Int64Passthrough) ToKindDetails() typing.KindDetails {
	return typing.Integer
}

func (Int64Passthrough) Convert(value any) (any, error) {
	if _, err := typing.AssertType[int64](value); err != nil {
		return nil, err
	}

	return value, nil
}

type Base64 struct{}

func (Base64) ToKindDetails() typing.KindDetails {
	// We're returning this back as a base64 encoded string.
	return typing.String
}

func (Base64) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[[]byte](value)
	if err != nil {
		return nil, err
	}

	return base64.StdEncoding.EncodeToString(castedValue), nil
}

// Float64 converter is used when Debezium's double.handling.mode is set to double.
type Float64 struct{}

func (Float64) ToKindDetails() typing.KindDetails {
	return typing.Float
}

func (Float64) Convert(value any) (any, error) {
	switch castedValue := value.(type) {
	case int:
		return float64(castedValue), nil
	case int64:
		return float64(castedValue), nil
	case int32:
		return float64(castedValue), nil
	case float32:
		return float64(castedValue), nil
	case float64:
		return castedValue, nil
	case string:
		if castedValue == "NaN" {
			return nil, nil
		}

		return nil, fmt.Errorf("unexpected type %T, with value %q", value, castedValue)
	default:
		return nil, fmt.Errorf("unexpected type %T", value)
	}
}

func NewArray(converter ValueConverter) Array {
	return Array{converter: converter}
}

type Array struct {
	converter ValueConverter
}

func (Array) ToKindDetails() typing.KindDetails {
	return typing.Array
}

func (a Array) Convert(value any) (any, error) {
	if fmt.Sprint(value) == fmt.Sprintf("[%s]", constants.ToastUnavailableValuePlaceholder) {
		return constants.ToastUnavailableValuePlaceholder, nil
	}

	// If there's no converter, just return the value as is.
	if a.converter == nil {
		return value, nil
	}

	elements, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected []any, got %T", value)
	}

	convertedElements := make([]any, len(elements))
	for i, element := range elements {
		convertedElement, err := a.converter.Convert(element)
		if err != nil {
			return nil, err
		}

		convertedElements[i] = convertedElement
	}

	return convertedElements, nil
}
