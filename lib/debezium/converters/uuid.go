package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/typing"
)

type String struct{}

func (String) Convert(value any) (any, error) {
	valueString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	return valueString, nil
}

func (String) ToKindDetails() typing.KindDetails {
	return typing.String
}
