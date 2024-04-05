package numbers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBetweenEq(t *testing.T) {
	type _tc struct {
		result bool
		start  int
		end    int
		number int
	}

	tcs := []_tc{
		{result: true, start: 5, end: 500, number: 100},
		{result: true, start: 5, end: 500, number: 5},
		{result: true, start: 5, end: 500, number: 500},
		{result: false, start: 5, end: 500, number: 501},
		{result: false, start: 5, end: 500, number: 4},
	}

	for _, tc := range tcs {
		assert.Equal(t, tc.result, BetweenEq(tc.start, tc.end, tc.number), tc)
	}
}
