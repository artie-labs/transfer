package converters

import "github.com/artie-labs/transfer/lib/typing"

type String struct{}

func (String) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	return castedValue, nil
}

func (String) ToKindDetails() typing.KindDetails {
	return typing.String
}
