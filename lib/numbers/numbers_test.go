package numbers

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
		assert.Equal(t, _case.result, BetweenEq(_case.start, _case.end, _case.number), _case)
	}
}
