package decimal

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/numbers"
)

func TestNewDecimal(t *testing.T) {
	assert.Equal(t, "0", NewDecimal(numbers.MustParseDecimal("0")).String())
	assert.Equal(t, "1", NewDecimal(numbers.MustParseDecimal("1")).String())
	assert.Equal(t, "12.34", NewDecimal(numbers.MustParseDecimal("12.34")).String())
}

func TestNewDecimalWithPrecision(t *testing.T) {
	// Precision = -1 (PrecisionNotSpecified):
	assert.Equal(t, Details{scale: 2, precision: -1}, NewDecimalWithPrecision(numbers.MustParseDecimal("12.34"), PrecisionNotSpecified).Details())
	// Precision = scale:
	assert.Equal(t, Details{scale: 2, precision: 2}, NewDecimalWithPrecision(numbers.MustParseDecimal("12.34"), 2).Details())
	// Precision < scale:
	assert.Equal(t, Details{scale: 2, precision: 3}, NewDecimalWithPrecision(numbers.MustParseDecimal("12.34"), 1).Details())
	// Precision > scale:
	assert.Equal(t, Details{scale: 2, precision: 4}, NewDecimalWithPrecision(numbers.MustParseDecimal("12.34"), 4).Details())
}

func TestDecimal_Details(t *testing.T) {
	// -1 precision (PrecisionNotSpecified):
	assert.Equal(t, Details{scale: 0, precision: -1}, NewDecimal(numbers.MustParseDecimal("0")).Details())
	assert.Equal(t, Details{scale: 0, precision: -1}, NewDecimal(numbers.MustParseDecimal("12345")).Details())
	assert.Equal(t, Details{scale: 0, precision: -1}, NewDecimal(numbers.MustParseDecimal("-12")).Details())
	assert.Equal(t, Details{scale: 2, precision: -1}, NewDecimal(numbers.MustParseDecimal("12345.12")).Details())
	assert.Equal(t, Details{scale: 3, precision: -1}, NewDecimal(numbers.MustParseDecimal("-12345.123")).Details())

	// 10 precision:
	assert.Equal(t, Details{scale: 0, precision: 10}, NewDecimalWithPrecision(numbers.MustParseDecimal("0"), 10).Details())
	assert.Equal(t, Details{scale: 0, precision: 10}, NewDecimalWithPrecision(numbers.MustParseDecimal("12345"), 10).Details())
	assert.Equal(t, Details{scale: 0, precision: 10}, NewDecimalWithPrecision(numbers.MustParseDecimal("-12"), 10).Details())
	assert.Equal(t, Details{scale: 2, precision: 10}, NewDecimalWithPrecision(numbers.MustParseDecimal("12345.12"), 10).Details())
	assert.Equal(t, Details{scale: 3, precision: 10}, NewDecimalWithPrecision(numbers.MustParseDecimal("-12345.123"), 10).Details())
}

func TestMarshalJSON(t *testing.T) {
	{
		// Zero
		bytes, err := NewDecimal(numbers.MustParseDecimal("0")).MarshalJSON()
		assert.NoError(t, err)
		assert.Equal(t, `"0"`, string(bytes))
	}
	{
		// As a nested object
		type Object struct {
			Decimal *Decimal `json:"decimal"`
			Foo     string   `json:"foo"`
		}

		var obj Object
		obj.Decimal = NewDecimal(numbers.MustParseDecimal("0"))
		obj.Foo = "bar"

		bytes, err := json.Marshal(obj)
		assert.NoError(t, err)
		assert.Equal(t, `{"decimal":"0","foo":"bar"}`, string(bytes))
	}
}
