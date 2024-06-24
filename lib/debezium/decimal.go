package debezium

import (
	"fmt"
	"math/big"
	"slices"

	"github.com/artie-labs/transfer/lib/typing/decimal"
)

// EncodeDecimal is used to encode a string representation of a number to `org.apache.kafka.connect.data.Decimal`.
func EncodeDecimal(value string, scale uint16) ([]byte, error) {
	bigFloatValue := new(big.Float)
	if _, success := bigFloatValue.SetString(value); !success {
		return nil, fmt.Errorf("unable to use %q as a floating-point number", value)
	}

	scaledValue := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	bigFloatValue.Mul(bigFloatValue, new(big.Float).SetInt(scaledValue))

	// Extract the scaled integer value.
	bigIntValue := new(big.Int)
	if _, success := bigIntValue.SetString(bigFloatValue.String(), 10); !success {
		return nil, fmt.Errorf("unable to use %q as a floating-point number", value)
	}

	data := bigIntValue.Bytes() // [Bytes] returns the absolute value of the number.
	if bigIntValue.Sign() < 0 {
		// Convert to two's complement if the number is negative

		if data[0] > 127 {
			// Prepend an empty byte if the first bit is already set.
			data = slices.Concat([]byte{0}, data)
		}

		// Inverting bits for two's complement.
		for i := range data {
			data[i] = ^data[i]
		}

		// Adding one to complete two's complement.
		twoComplement := new(big.Int).SetBytes(data)
		twoComplement.Add(twoComplement, big.NewInt(1))

		data = twoComplement.Bytes()
		if data[0]&0x80 == 0 {
			// 0xff is -1 in Java
			// https://stackoverflow.com/questions/1677957/why-byte-b-byte-0xff-is-equals-to-integer-1
			data = append([]byte{0xff}, data...)
		}
	} else {
		// For positive values, prepend a zero if the highest bit is set to ensure it's interpreted as positive.
		if len(data) > 0 && data[0]&0x80 != 0 {
			data = append([]byte{0x00}, data...)
		}
	}
	return data, nil
}

// DecodeDecimal is used to decode `org.apache.kafka.connect.data.Decimal`.
func DecodeDecimal(data []byte, precision *int, scale int) *decimal.Decimal {
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

	// Convert the big integer to a big float
	bigFloat := new(big.Float).SetInt(bigInt)

	// Compute divisor as 10^scale with big.Int's Exp, then convert to big.Float
	scaleInt := big.NewInt(int64(scale))
	ten := big.NewInt(10)
	divisorInt := new(big.Int).Exp(ten, scaleInt, nil)
	divisorFloat := new(big.Float).SetInt(divisorInt)

	// Perform the division
	bigFloat.Quo(bigFloat, divisorFloat)
	return decimal.NewDecimal(precision, scale, bigFloat)
}
