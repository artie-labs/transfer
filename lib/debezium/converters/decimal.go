package converters

import (
	"encoding/base64"
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium/encode"

	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

// toBytes attempts to convert a value (type []byte, or string) to a slice of bytes.
// - If value is already a slice of bytes it will be directly returned.
// - If value is a string we will attempt to base64 decode it.
func toBytes(value any) ([]byte, error) {
	switch typedValue := value.(type) {
	case []byte:
		return typedValue, nil
	case string:
		data, err := base64.StdEncoding.DecodeString(typedValue)
		if err != nil {
			return nil, fmt.Errorf("failed to base64 decode: %w", err)
		}

		return data, nil
	default:
		return nil, fmt.Errorf("failed to cast value '%v' with type '%T' to []byte", value, value)
	}
}

type Decimal struct {
	precision int32
	scale     int32

	variableNumeric bool
}

func NewDecimal(precision int32, scale int32, variableNumeric bool) *Decimal {
	return &Decimal{
		precision:       precision,
		scale:           scale,
		variableNumeric: variableNumeric,
	}
}

func (d Decimal) ToKindDetails() typing.KindDetails {
	return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(d.precision, d.scale))
}

func (d Decimal) Convert(val any) (any, error) {
	if d.variableNumeric {
		valueStruct, isOk := val.(map[string]any)
		if !isOk {
			return nil, fmt.Errorf("value is not map[string]any type")
		}

		scale, err := maputil.GetInt32FromMap(valueStruct, "scale")
		if err != nil {
			return nil, err
		}

		val, isOk := valueStruct["value"]
		if !isOk {
			return nil, fmt.Errorf("encoded value does not exist")
		}

		bytes, err := toBytes(val)
		if err != nil {
			return nil, err
		}

		return decimal.NewDecimal(encode.DecodeDecimal(bytes, scale)), nil
	} else {
		bytes, err := toBytes(val)
		if err != nil {
			return nil, err
		}

		return decimal.NewDecimalWithPrecision(encode.DecodeDecimal(bytes, d.scale), d.precision), nil
	}
}
