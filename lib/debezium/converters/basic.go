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
