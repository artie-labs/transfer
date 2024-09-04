package debezium

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbzConverters "github.com/artie-labs/transfer/lib/debezium/converters"
)

func BenchmarkDecodeDecimal_P64_S10(b *testing.B) {
	parameters := map[string]any{
		"scale":                  10,
		KafkaDecimalPrecisionKey: 64,
	}
	field := Field{Parameters: parameters}
	for i := 0; i < b.N; i++ {
		bytes, err := dbzConverters.Bytes{}.Convert("AwBGAw8m9GLXrCGifrnVP/8jPHrNEtd1r4rS")
		assert.NoError(b, err)
		dec, err := field.DecodeDecimal(bytes.([]byte))
		assert.NoError(b, err)
		assert.Equal(b, "123456789012345678901234567890123456789012345678901234.1234567890", dec.String())
		require.NoError(b, err)
	}
}

func BenchmarkDecodeDecimal_P38_S2(b *testing.B) {
	parameters := map[string]any{
		"scale":                  2,
		KafkaDecimalPrecisionKey: 38,
	}
	field := Field{Parameters: parameters}
	for i := 0; i < b.N; i++ {
		bytes, err := dbzConverters.Bytes{}.Convert(`AMCXznvJBxWzS58P/////w==`)
		assert.NoError(b, err)
		dec, err := field.DecodeDecimal(bytes.([]byte))
		assert.NoError(b, err)
		assert.Equal(b, "9999999999999999999999999999999999.99", dec.String())
	}
}

func BenchmarkDecodeDecimal_P5_S2(b *testing.B) {
	parameters := map[string]any{
		"scale":                  2,
		KafkaDecimalPrecisionKey: 5,
	}

	field := Field{Parameters: parameters}
	for i := 0; i < b.N; i++ {
		bytes, err := dbzConverters.Bytes{}.Convert(`AOHJ`)
		assert.NoError(b, err)
		dec, err := field.DecodeDecimal(bytes.([]byte))
		assert.NoError(b, err)
		assert.Equal(b, "578.01", dec.String())
	}
}
