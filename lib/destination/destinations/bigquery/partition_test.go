package bigquery

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDistinctDates(t *testing.T) {
	{
		// Invalid date
		dates, err := buildDistinctDates("ts", []map[string]any{
			{"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)},
			{"ts": nil},
		})
		assert.ErrorContains(t, err, `column "ts" is not a time column`)
		assert.Empty(t, dates)
	}
	{
		// No dates
		dates, err := buildDistinctDates("", nil)
		assert.NoError(t, err)
		assert.Empty(t, dates)
	}
	{
		// One date
		dates, err := buildDistinctDates("ts", []map[string]any{
			{"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)},
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{"2020-01-01"}, dates)
	}
	{
		// Two dates
		dates, err := buildDistinctDates("ts", []map[string]any{
			{"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)},
			{"ts": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)},
		})
		assert.NoError(t, err)
		equalLists(t, []string{"2020-01-01", "2020-01-02"}, dates)
	}
	{
		// Three days, two unique
		dates, err := buildDistinctDates("ts", []map[string]any{
			{"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)},
			{"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)},
			{"ts": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)},
		})
		assert.NoError(t, err)
		equalLists(t, []string{"2020-01-01", "2020-01-02"}, dates)
	}
}

func equalLists(t *testing.T, list1 []string, list2 []string) {
	// Sort the two lists prior to comparison
	slices.Sort(list1)
	slices.Sort(list2)
	assert.Equal(t, list1, list2)
}
