package values

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/converters"
)

func ToStringOpts(colVal any, colKind typing.KindDetails, opts converters.GetStringConverterOpts) (string, error) {
	if colVal == nil {
		return "", fmt.Errorf("colVal is nil")
	}

	sv, err := converters.GetStringConverter(colKind, opts)
	if err != nil {
		return "", fmt.Errorf("failed to get string converter: %w", err)
	}

	value, err := sv.Convert(colVal)
	if err != nil {
		return "", fmt.Errorf("converter %T failed to convert value: %w", sv, err)
	}

	return value, nil
}

func ToString(colVal any, colKind typing.KindDetails) (string, error) {
	return ToStringOpts(colVal, colKind, converters.GetStringConverterOpts{})
}
