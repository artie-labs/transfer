package converters

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

type Decimal struct {
	details decimal.Details
}

func (d Decimal) ToKindDetails() typing.KindDetails {
	return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, d.details)
}

func (d Decimal) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[[]byte](value)
	if err != nil {
		return nil, err
	}

	_decimal := debezium.DecodeDecimal(castedValue, d.details.Scale())
	if d.details.Precision() == decimal.PrecisionNotSpecified {
		return decimal.NewDecimal(_decimal), nil
	}

	return decimal.NewDecimalWithPrecision(_decimal, d.details.Precision()), nil
}

func NewDecimal(details decimal.Details) *Decimal {
	return &Decimal{
		details: details,
	}
}
