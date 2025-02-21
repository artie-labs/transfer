package values

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/converters"
)

func ToString(colVal any, colKind typing.KindDetails, opts converters.GetStringConverterOpts) (string, error) {
	if colVal == nil {
		return "", fmt.Errorf("colVal is nil")
	}

	sv, err := converters.GetStringConverter(colKind, opts)
	if err != nil {
		return "", fmt.Errorf("failed to get string converter: %w", err)
	}

	return sv.Convert(colVal)
}
