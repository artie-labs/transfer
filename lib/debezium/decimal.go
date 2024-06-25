package debezium

import (
	"fmt"
	"math/big"
	"slices"

	"github.com/artie-labs/transfer/lib/typing/decimal"
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

// EncodeDecimal is used to encode a string representation of a number to `org.apache.kafka.connect.data.Decimal`.
func EncodeDecimal(value string, scale uint16) ([]byte, error) {
	bigFloatValue := new(big.Float)
	if _, success := bigFloatValue.SetString(value); !success {
		return nil, fmt.Errorf("unable to use %q as a floating-point number", value)
	}

	if scale > 0 {
		scaledValue := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
		bigFloatValue.Mul(bigFloatValue, new(big.Float).SetInt(scaledValue))
	}

	bigIntValue := new(big.Int)
	if bigFloatValue.IsInt() {
		bigFloatValue.Int(bigIntValue)
	} else {
		strValue := bigFloatValue.Text('f', 0)
		if _, success := bigIntValue.SetString(strValue, 10); !success {
			return nil, fmt.Errorf("unable to use %q as a big.Int", strValue)
		}
	}

	return encodeBigInt(bigIntValue), nil
}

// DecodeDecimal is used to decode `org.apache.kafka.connect.data.Decimal`.
func DecodeDecimal(data []byte, precision *int, scale int) *decimal.Decimal {
	// Convert the big integer to a big float
	bigFloat := new(big.Float).SetInt(decodeBigInt(data))

	// Compute divisor as 10^scale with big.Int's Exp, then convert to big.Float
	scaleInt := big.NewInt(int64(scale))
	ten := big.NewInt(10)
	divisorInt := new(big.Int).Exp(ten, scaleInt, nil)
	divisorFloat := new(big.Float).SetInt(divisorInt)

	// Perform the division
	bigFloat.Quo(bigFloat, divisorFloat)
	return decimal.NewDecimal(precision, scale, bigFloat)
}
