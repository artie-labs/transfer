package converters

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

func BenchmarkDecodeDecimal_P64_S10(b *testing.B) {
	converter := NewDecimal(64, 10, false)
	for i := 0; i < b.N; i++ {
		val, err := converter.Convert("AwBGAw8m9GLXrCGifrnVP/8jPHrNEtd1r4rS")
		assert.NoError(b, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(b, isOk)
		assert.Equal(b, "123456789012345678901234567890123456789012345678901234.1234567890", dec.String())
	}
}

func BenchmarkDecodeDecimal_P38_S2(b *testing.B) {
	converter := NewDecimal(38, 2, false)
	for i := 0; i < b.N; i++ {
		val, err := converter.Convert("AMCXznvJBxWzS58P/////w==")
		assert.NoError(b, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(b, isOk)
		assert.Equal(b, "9999999999999999999999999999999999.99", dec.String())
	}
}

func BenchmarkDecodeDecimal_P5_S2(b *testing.B) {
	converter := NewDecimal(5, 2, false)
	for i := 0; i < b.N; i++ {
		val, err := converter.Convert("AOHJ")
		assert.NoError(b, err)

		dec, isOk := val.(*decimal.Decimal)
		assert.True(b, isOk)
		assert.Equal(b, "578.01", dec.String())
	}
}
