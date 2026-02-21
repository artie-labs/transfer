package converters

import "github.com/artie-labs/transfer/lib/typing"

type Interval struct{}

func (Interval) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	return castedValue, nil
}

func (Interval) ToKindDetails() typing.KindDetails {
	return typing.Interval
}
