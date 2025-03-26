package numbers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBetweenEq(t *testing.T) {
	{
		// Test number within range
		assert.True(t, BetweenEq(5, 500, 100), "number within range should return true")
	}
	{
		// Test number at lower bound
		assert.True(t, BetweenEq(5, 500, 5), "number at lower bound should return true")
	}
	{
		// Test number at upper bound
		assert.True(t, BetweenEq(5, 500, 500), "number at upper bound should return true")
	}
	{
		// Test number above range
		assert.False(t, BetweenEq(5, 500, 501), "number above range should return false")
	}
	{
		// Test number below range
		assert.False(t, BetweenEq(5, 500, 4), "number below range should return false")
	}
}
