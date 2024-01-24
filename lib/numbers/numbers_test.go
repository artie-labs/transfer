package numbers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBetweenEq(t *testing.T) {
	type testCase struct {
		result bool
		start  int
		end    int
		number int
	}

	cases := []testCase{
		{result: true, start: 5, end: 500, number: 100},
		{result: true, start: 5, end: 500, number: 5},
		{result: true, start: 5, end: 500, number: 500},
		{result: false, start: 5, end: 500, number: 501},
		{result: false, start: 5, end: 500, number: 4},
	}

	for _, _case := range cases {
		assert.Equal(t, _case.result, BetweenEq(BetweenEqArgs{
			Start:  _case.start,
			End:    _case.end,
			Number: _case.number,
		}), _case)
	}
}

func TestFloatToString(t *testing.T) {
	{
		assert.Equal(t, "1", Float64ToString(1.0))
		assert.Equal(t, "1.1", Float64ToString(1.1))
		assert.Equal(t, "1533369.333", Float64ToString(1533369.333))
	}
}
