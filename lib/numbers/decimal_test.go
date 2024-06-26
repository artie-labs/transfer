package numbers

import (
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/stretchr/testify/assert"
)

func TestDecimalWithNewExponent(t *testing.T) {
	assert.Equal(t, "0", DecimalWithNewExponent(apd.New(0, 0), 0).Text('f'))
	assert.Equal(t, "00", DecimalWithNewExponent(apd.New(0, 1), 1).Text('f'))
	assert.Equal(t, "0", DecimalWithNewExponent(apd.New(0, 100), 0).Text('f'))
	assert.Equal(t, "00", DecimalWithNewExponent(apd.New(0, 0), 1).Text('f'))
	assert.Equal(t, "0.0", DecimalWithNewExponent(apd.New(0, 0), -1).Text('f'))

	// Same exponent:
	assert.Equal(t, "12.349", DecimalWithNewExponent(MustParseDecimal("12.349"), -3).Text('f'))
	// More precise exponent:
	assert.Equal(t, "12.3490", DecimalWithNewExponent(MustParseDecimal("12.349"), -4).Text('f'))
	assert.Equal(t, "12.34900", DecimalWithNewExponent(MustParseDecimal("12.349"), -5).Text('f'))
	// Lest precise exponent:
	// Extra digits should be truncated rather than rounded.
	assert.Equal(t, "12.34", DecimalWithNewExponent(MustParseDecimal("12.349"), -2).Text('f'))
	assert.Equal(t, "12.3", DecimalWithNewExponent(MustParseDecimal("12.349"), -1).Text('f'))
	assert.Equal(t, "12", DecimalWithNewExponent(MustParseDecimal("12.349"), 0).Text('f'))
	assert.Equal(t, "10", DecimalWithNewExponent(MustParseDecimal("12.349"), 1).Text('f'))
}
