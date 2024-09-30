package typing

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseNumeric(t *testing.T) {
	{
		// Invalid
		{
			result, err := ParseNumeric([]string{})
			assert.ErrorContains(t, err, "invalid number of parts: 0")
			assert.Equal(t, Invalid, result)
		}
		{
			result, err := ParseNumeric([]string{"5", "a"})
			assert.ErrorContains(t, err, `failed to parse number, part "a"`)
			assert.Equal(t, Invalid, result)
		}
		{
			result := ParseNumeric([]string{"b", "5"})
			assert.Equal(t, Invalid, result)
		}
		{
			result := ParseNumeric([]string{"a", "b"})
			assert.Equal(t, Invalid, result)
		}
		{
			result := ParseNumeric([]string{"1", "2", "3"})
			assert.Equal(t, Invalid, result)
		}
		{
			// Test values that are larger than [math.MaxInt32].
			assert.Equal(t, Invalid, ParseNumeric([]string{"10", fmt.Sprint(math.MaxInt32 + 1)}))
			assert.Equal(t, Invalid, ParseNumeric([]string{fmt.Sprint(math.MaxInt32 + 1), "10"}))
		}
	}
	{
		// Decimals
		{
			result := ParseNumeric([]string{"5", "2"})
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(5), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), result.ExtendedDecimalDetails.Scale())
		}
		{
			result := ParseNumeric([]string{"5", "  2     "})
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(5), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), result.ExtendedDecimalDetails.Scale())
		}
		{
			result := ParseNumeric([]string{"39", "6"})
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(39), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(6), result.ExtendedDecimalDetails.Scale())
		}
		{
			result := ParseNumeric([]string{fmt.Sprint(math.MaxInt32), fmt.Sprint(math.MaxInt32)})
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(math.MaxInt32), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(math.MaxInt32), result.ExtendedDecimalDetails.Scale())
		}
	}
	{
		// Integer
		{
			result := ParseNumeric([]string{"5"})
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(5), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), result.ExtendedDecimalDetails.Scale())
		}
		{
			result := ParseNumeric([]string{"5", "0"})
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(5), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), result.ExtendedDecimalDetails.Scale())
		}
	}
}
