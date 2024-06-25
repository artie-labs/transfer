package main

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/artie-labs/transfer/lib/debezium"
)

func mustEncodeAndDecodeDecimal(value string, scale uint16) string {
	bytes, err := debezium.EncodeDecimal(value, scale)
	if err != nil {
		panic(err)
	}
	return debezium.DecodeDecimal(bytes, nil, int(scale)).String()
}

func randDigit() (byte, bool) {
	offset := rand.Intn(10)
	return byte(48 + offset), offset == 0
}

func generateNumberWithScale(maxDigitsBefore int, maxDigitsAfter int) (string, int) {
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

	if wroteNonZero && rand.Intn(2) == 1 {
		return "-" + out.String(), scale
	}

	return out.String(), scale
}

func main() {
	for i := range 1000 {
		fmt.Printf("Checking batch %d...\n", i)
		for range 1_000_000 {
			in, scale := generateNumberWithScale(15, 15)
			out := mustEncodeAndDecodeDecimal(in, uint16(scale))
			if in != out {
				panic(fmt.Sprintf("Failed for %s -> %s", in, out))
			}
		}
	}
}
