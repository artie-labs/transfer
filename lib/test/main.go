package main

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/cockroachdb/apd/v3"
)

func mustEncodeAndDecodeDecimal(decimal *apd.Decimal, scale int32) string {
	bytes := debezium.EncodeDecimalWithScale(decimal, scale)
	return debezium.DecodeDecimal(bytes, scale).Text('f')
}

func randDigit() (byte, bool) {
	offset := rand.Intn(10)
	return byte(48 + offset), offset == 0
}

func generateNumberWithScale(maxDigitsBefore int, maxDigitsAfter int) (*apd.Decimal, int32) {
	out := strings.Builder{}

	var wroteNonZero bool
	for range rand.Intn(maxDigitsBefore + 1) {
		digit, isZero := randDigit()
		if isZero && !wroteNonZero {
			continue
		}
		wroteNonZero = true
		out.WriteByte(digit)
	}

	if !wroteNonZero {
		out.WriteRune('0')
	}

	scale := rand.Intn(maxDigitsAfter + 1)
	if scale > 0 {
		out.WriteRune('.')

		for range scale {
			digit, isZero := randDigit()
			if !isZero {
				wroteNonZero = true
			}
			out.WriteByte(digit)
		}
	}

	stringValue := out.String()

	if wroteNonZero && rand.Intn(2) == 1 {
		stringValue = "-" + stringValue
	}

	decimal, _, err := apd.NewFromString(stringValue)
	if err != nil {
		panic(err)
	}
	return decimal, -decimal.Exponent
}

func main() {
	for i := range 1000 {
		fmt.Printf("Checking batch %d...\n", i)
		for range 1_000_000 {
			in, scale := generateNumberWithScale(30, 30)
			out := mustEncodeAndDecodeDecimal(in, scale)
			if in.Text('f') != out {
				panic(fmt.Sprintf("Failed for %s -> %s", in.Text('f'), out))
			}
		}
	}
}
