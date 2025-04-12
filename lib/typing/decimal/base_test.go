package decimal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecimal_IsNumeric(t *testing.T) {
	{
		// Valid numeric with small scale
		assert.True(t, NewDetails(10, 2).isNumeric(), "should be valid numeric with small scale")
	}
	{
		// Valid numeric with max scale
		assert.True(t, NewDetails(38, 9).isNumeric(), "should be valid numeric with max scale")
	}
	{
		// Invalid - precision not specified
		assert.False(t, NewDetails(PrecisionNotSpecified, 2).isNumeric(), "should be invalid when precision is not specified")
	}
	{
		// Invalid - scale too large
		assert.False(t, NewDetails(10, 10).isNumeric(), "should be invalid when scale is too large")
	}
	{
		// Invalid - precision too small
		assert.False(t, NewDetails(1, 2).isNumeric(), "should be invalid when precision is too small")
	}
	{
		// Invalid - precision too large
		assert.False(t, NewDetails(40, 2).isNumeric(), "should be invalid when precision is too large")
	}
	{
		// Valid - minimum valid case
		assert.True(t, NewDetails(1, 0).isNumeric(), "should be valid with minimum precision and scale")
	}
	{
		// Valid - scale equals precision
		assert.True(t, NewDetails(5, 5).isNumeric(), "should be valid when scale equals precision")
	}
}

func TestDecimal_IsBigNumeric(t *testing.T) {
	{
		// Valid bignumeric with small scale
		assert.True(t, NewDetails(39, 2).isBigNumeric(), "should be valid bignumeric with small scale")
	}
	{
		// Valid bignumeric with max scale
		assert.True(t, NewDetails(77, 38).isBigNumeric(), "should be valid bignumeric with max scale")
	}
	{
		// Invalid - precision not specified
		assert.False(t, NewDetails(PrecisionNotSpecified, 2).isBigNumeric(), "should be invalid when precision is not specified")
	}
	{
		// Invalid - scale too large
		assert.False(t, NewDetails(39, 39).isBigNumeric(), "should be invalid when scale is too large")
	}
	{
		// Invalid - precision too small
		assert.False(t, NewDetails(38, 2).isBigNumeric(), "should be invalid when precision is too small")
	}
	{
		// Valid - minimum valid case
		assert.True(t, NewDetails(39, 0).isBigNumeric(), "should be valid with minimum precision and scale")
	}
	{
		// Valid - scale equals precision
		assert.True(t, NewDetails(39, 39).isBigNumeric(), "should be valid when scale equals precision")
	}
}
