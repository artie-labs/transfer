package converters

import "github.com/artie-labs/transfer/lib/typing"

type StringPassthrough struct{}

func (StringPassthrough) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	return castedValue, nil
}

func (StringPassthrough) ToKindDetails() (typing.KindDetails, error) {
	return typing.String, nil
}
