package converters

import (
	"fmt"
	"math/big"
	"slices"

	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/cockroachdb/apd/v3"
)

// encodeBigInt encodes a [big.Int] into a big-endian byte slice using two's complement.
func encodeBigInt(value *big.Int) []byte {
	data := value.Bytes() // [Bytes] returns the absolute value of the number.

	if len(data) == 0 {
		return data
	}

	if data[0] >= 0x80 {
		// If the leftmost bit is already set then it is a significant bit and we need to prepend an additional byte
		// so that the leftmost bit can safely be used to indicate whether the number is positive or negative.
		data = slices.Concat([]byte{0x00}, data)
	}

	if value.Sign() < 0 {
		// Convert to two's complement if the number is negative

		// Inverting bits for two's complement.
		for i := range data {
			data[i] = ^data[i]
		}

		// Adding one to complete two's complement.
		twoComplement := new(big.Int).SetBytes(data)
		twoComplement.Add(twoComplement, big.NewInt(1))

		return twoComplement.Bytes()
	}

	return data
}

// decodeBigInt decodes a [big.Int] from a big-endian byte slice that has been encoded using two's complement.
func decodeBigInt(data []byte) *big.Int {
	bigInt := new(big.Int)

	// If the data represents a negative number, the sign bit will be set.
	if len(data) > 0 && data[0] >= 0x80 {
		// To convert the data to a two's complement integer, we need to invert the bytes and add one.
		for i := range data {
			data[i] = ^data[i]
		}

		bigInt.SetBytes(data)
		// We are adding this because Debezium (Java) encoded this value and uses two's complement binary representation for negative numbers
		bigInt.Add(bigInt, big.NewInt(1))
		bigInt.Neg(bigInt)
	} else {
		bigInt.SetBytes(data)
	}

	return bigInt
}

// EncodeDecimal is used to encode a [*apd.Decimal] to `org.apache.kafka.connect.data.Decimal`.
// The scale of the value (which is the negated exponent of the decimal) is returned as the second argument.
func EncodeDecimal(decimal *apd.Decimal) ([]byte, int32) {
	bigIntValue := decimal.Coeff.MathBigInt()
	if decimal.Negative {
		bigIntValue.Neg(bigIntValue)
	}

	return encodeBigInt(bigIntValue), -decimal.Exponent
}

// DecodeDecimal is used to decode `org.apache.kafka.connect.data.Decimal`.
func DecodeDecimal(data []byte, scale int32) *apd.Decimal {
	bigInt := new(apd.BigInt).SetMathBigInt(decodeBigInt(data))
	return apd.NewWithBigInt(bigInt, -scale)
}

type VariableDecimal struct {
	details decimal.Details
}

func (v VariableDecimal) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, v.details), nil
}

func (v VariableDecimal) Convert(value any) (any, error) {
	valueStruct, isOk := value.(map[string]any)
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

	bytes, err := Bytes{}.Convert(val)
	if err != nil {
		return nil, err
	}

	castedBytes, err := typing.AssertType[[]byte](bytes)
	if err != nil {
		return nil, err
	}

	return decimal.NewDecimal(DecodeDecimal(castedBytes, scale)), nil
}

func NewVariableDecimal() VariableDecimal {
	// For variable numeric types, we are defaulting to a scale of 5
	// This is because scale is not specified at the column level, rather at the row level
	// It shouldn't matter much anyway since the column type we are creating is `TEXT` to avoid boundary errors.
	return VariableDecimal{details: decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)}
}

type Decimal struct {
	details decimal.Details
}

func NewDecimal(details decimal.Details) Decimal {
	return Decimal{details: details}
}

func (d Decimal) ToKindDetails() (typing.KindDetails, error) {
	return typing.NewDecimalDetailsFromTemplate(typing.EDecimal, d.details), nil
}

func (d Decimal) Convert(value any) (any, error) {
	castedBytes, err := typing.AssertType[[]byte](value)
	if err != nil {
		return nil, err
	}

	return decimal.NewDecimalWithPrecision(DecodeDecimal(castedBytes, d.details.Scale()), d.details.Precision()), nil
}
