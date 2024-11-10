package bigquery

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDistinctDates(t *testing.T) {
	testCases := []struct {
		name                string
		rowData             []map[string]any // pk -> { col -> val }
		expectedErr         string
		expectedDatesString []string
	}{
		{
			name: "no dates",
		},
		{
			name: "one date",
			rowData: []map[string]any{
				{
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
			},
			expectedDatesString: []string{"2020-01-01"},
		},
		{
			name: "two dates",
			rowData: []map[string]any{
				{
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
				{
					"ts": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
			},
			expectedDatesString: []string{"2020-01-01", "2020-01-02"},
		},
		{
			name: "3 dates, 2 unique",
			rowData: []map[string]any{
				{
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
				{
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
				{
					"ts": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
			},
			expectedDatesString: []string{"2020-01-01", "2020-01-02"},
		},
		{
			name: "two dates, one is nil",
			rowData: []map[string]any{
				{
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
				{
					"ts": nil,
				},
			},
			expectedErr: `column "ts" is not a time column`,
		},
	}

	for _, testCase := range testCases {
		actualValues, actualErr := buildDistinctDates("ts", testCase.rowData)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, actualErr, testCase.expectedErr, testCase.name)
		} else {
			assert.NoError(t, actualErr, testCase.name)
			assert.Equal(t, true, slicesEqualUnordered(testCase.expectedDatesString, actualValues),
				fmt.Sprintf("2 arrays not the same, test name: %s, expected array: %v, actual array: %v",
					testCase.name, testCase.expectedDatesString, actualValues))
		}
	}
}

func slicesEqualUnordered(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}

	slices.Sort(s1)
	slices.Sort(s2)

	for i, v := range s1 {
		if v != s2[i] {
			return false
		}
	}

	return true
}
