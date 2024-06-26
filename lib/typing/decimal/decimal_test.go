package decimal

import (
	"testing"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func TestNewDecimal(t *testing.T) {
	// Nil precision:
	assert.Equal(t, "0", NewDecimal(nil, numbers.MustParseDecimal("0")).String())
	// Precision = -1 (PrecisionNotSpecified):
	assert.Equal(t, DecimalDetails{scale: 2, precision: -1}, NewDecimal(ptr.ToInt32(-1), numbers.MustParseDecimal("12.34")).Details())
	// Precision = scale:
	assert.Equal(t, DecimalDetails{scale: 2, precision: 2}, NewDecimal(ptr.ToInt32(2), numbers.MustParseDecimal("12.34")).Details())
	// Precision < scale:
	assert.Equal(t, DecimalDetails{scale: 2, precision: 3}, NewDecimal(ptr.ToInt32(1), numbers.MustParseDecimal("12.34")).Details())
	// Precision > scale:
	assert.Equal(t, DecimalDetails{scale: 2, precision: 4}, NewDecimal(ptr.ToInt32(4), numbers.MustParseDecimal("12.34")).Details())
}

func TestDecimal_Scale(t *testing.T) {
	assert.Equal(t, int32(0), NewDecimal(nil, numbers.MustParseDecimal("0")).Scale())
	assert.Equal(t, int32(0), NewDecimal(nil, numbers.MustParseDecimal("12345")).Scale())
	assert.Equal(t, int32(0), NewDecimal(nil, numbers.MustParseDecimal("12300")).Scale())
	assert.Equal(t, int32(1), NewDecimal(nil, numbers.MustParseDecimal("12300.0")).Scale())
	assert.Equal(t, int32(2), NewDecimal(nil, numbers.MustParseDecimal("12300.00")).Scale())
	assert.Equal(t, int32(2), NewDecimal(nil, numbers.MustParseDecimal("12345.12")).Scale())
	assert.Equal(t, int32(3), NewDecimal(nil, numbers.MustParseDecimal("-12345.123")).Scale())
}

func TestDecimal_Details(t *testing.T) {
	// Nil precision:
	assert.Equal(t, DecimalDetails{scale: 0, precision: -1}, NewDecimal(nil, numbers.MustParseDecimal("0")).Details())
	assert.Equal(t, DecimalDetails{scale: 0, precision: -1}, NewDecimal(nil, numbers.MustParseDecimal("12345")).Details())
	assert.Equal(t, DecimalDetails{scale: 0, precision: -1}, NewDecimal(nil, numbers.MustParseDecimal("-12")).Details())
	assert.Equal(t, DecimalDetails{scale: 2, precision: -1}, NewDecimal(nil, numbers.MustParseDecimal("12345.12")).Details())
	assert.Equal(t, DecimalDetails{scale: 3, precision: -1}, NewDecimal(nil, numbers.MustParseDecimal("-12345.123")).Details())

	// -1 precision (PrecisionNotSpecified):
	assert.Equal(t, DecimalDetails{scale: 0, precision: -1}, NewDecimal(ptr.ToInt32(-1), numbers.MustParseDecimal("0")).Details())
	assert.Equal(t, DecimalDetails{scale: 0, precision: -1}, NewDecimal(ptr.ToInt32(-1), numbers.MustParseDecimal("12345")).Details())
	assert.Equal(t, DecimalDetails{scale: 0, precision: -1}, NewDecimal(ptr.ToInt32(-1), numbers.MustParseDecimal("-12")).Details())
	assert.Equal(t, DecimalDetails{scale: 2, precision: -1}, NewDecimal(ptr.ToInt32(-1), numbers.MustParseDecimal("12345.12")).Details())
	assert.Equal(t, DecimalDetails{scale: 3, precision: -1}, NewDecimal(ptr.ToInt32(-1), numbers.MustParseDecimal("-12345.123")).Details())

	// 10 precision:
	assert.Equal(t, DecimalDetails{scale: 0, precision: 10}, NewDecimal(ptr.ToInt32(10), numbers.MustParseDecimal("0")).Details())
	assert.Equal(t, DecimalDetails{scale: 0, precision: 10}, NewDecimal(ptr.ToInt32(10), numbers.MustParseDecimal("12345")).Details())
	assert.Equal(t, DecimalDetails{scale: 0, precision: 10}, NewDecimal(ptr.ToInt32(10), numbers.MustParseDecimal("-12")).Details())
	assert.Equal(t, DecimalDetails{scale: 2, precision: 10}, NewDecimal(ptr.ToInt32(10), numbers.MustParseDecimal("12345.12")).Details())
	assert.Equal(t, DecimalDetails{scale: 3, precision: 10}, NewDecimal(ptr.ToInt32(10), numbers.MustParseDecimal("-12345.123")).Details())
}
