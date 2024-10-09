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
			assert.ErrorContains(t, err, `failed to parse number: strconv.ParseInt: parsing "a": invalid syntax`)
			assert.Equal(t, Invalid, result)
		}
		{
			result, err := ParseNumeric([]string{"b", "5"})
			assert.ErrorContains(t, err, `failed to parse number: strconv.ParseInt: parsing "b": invalid syntax`)
			assert.Equal(t, Invalid, result)
		}
		{
			result, err := ParseNumeric([]string{"a", "b"})
			assert.ErrorContains(t, err, `failed to parse number: strconv.ParseInt: parsing "a"`)
			assert.Equal(t, Invalid, result)
		}
		{
			result, err := ParseNumeric([]string{"1", "2", "3"})
			assert.ErrorContains(t, err, `invalid number of parts: 3`)
			assert.Equal(t, Invalid, result)
		}
		{
			// Test values that are larger than [math.MaxInt32].
			{
				result, err := ParseNumeric([]string{"10", fmt.Sprint(math.MaxInt32 + 1)})
				assert.Equal(t, Invalid, result)
				assert.ErrorContains(t, err, `failed to parse number: strconv.ParseInt: parsing "2147483648": value out of range`)
			}
			{
				result, err := ParseNumeric([]string{fmt.Sprint(math.MaxInt32 + 1), "10"})
				assert.Equal(t, Invalid, result)
				assert.ErrorContains(t, err, `failed to parse number: strconv.ParseInt: parsing "2147483648": value out of range`)
			}
		}
	}
	{
		// Decimals
		{
			result, err := ParseNumeric([]string{"5", "2"})
			assert.NoError(t, err)
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(5), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), result.ExtendedDecimalDetails.Scale())
		}
		{
			result, err := ParseNumeric([]string{"5", "  2     "})
			assert.NoError(t, err)
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(5), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), result.ExtendedDecimalDetails.Scale())
		}
		{
			result, err := ParseNumeric([]string{"39", "6"})
			assert.NoError(t, err)
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(39), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(6), result.ExtendedDecimalDetails.Scale())
		}
		{
			result, err := ParseNumeric([]string{fmt.Sprint(math.MaxInt32), fmt.Sprint(math.MaxInt32)})
			assert.NoError(t, err)
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(math.MaxInt32), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(math.MaxInt32), result.ExtendedDecimalDetails.Scale())
		}
	}
	{
		// Integer
		{
			result, err := ParseNumeric([]string{"5"})
			assert.NoError(t, err)
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(5), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), result.ExtendedDecimalDetails.Scale())
		}
		{
			result, err := ParseNumeric([]string{"5", "0"})
			assert.NoError(t, err)
			assert.Equal(t, EDecimal.Kind, result.Kind)
			assert.Equal(t, int32(5), result.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), result.ExtendedDecimalDetails.Scale())
		}
	}
}
