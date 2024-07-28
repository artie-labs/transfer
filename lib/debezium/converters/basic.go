package converters

import (
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
