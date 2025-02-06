package values

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/converters"
)

func ToString(colVal any, colKind typing.KindDetails) (string, error) {
	if colVal == nil {
		return "", fmt.Errorf("colVal is nil")
	}

	sv, err := converters.GetStringConverter(colKind)
	if err != nil {
		return "", fmt.Errorf("failed to get string converter: %w", err)
	}

	// TODO: Simplify this block
	if sv != nil {
		return sv.Convert(colVal)
	}

	return fmt.Sprint(colVal), nil
}
