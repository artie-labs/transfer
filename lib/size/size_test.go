package size

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetApproxSize(t *testing.T) {
	rowsData := make(map[string]any) // pk -> { col -> val }
	for i := 0; i < 500; i++ {
		rowsData[fmt.Sprintf("key-%v", i)] = map[string]any{
			"id":         fmt.Sprintf("key-%v", i),
			"artie":      "transfer",
			"dusty":      "the mini aussie",
			"next_puppy": true,
			"foo":        []any{"bar", "baz", "qux"},
			"team":       []string{"charlie", "robin", "jacqueline"},
			"arrays":     []string{"foo", "bar", "baz"},
			"nested": map[string]any{
				"foo": "bar",
				"abc": "xyz",
			},
			"array_of_maps": []map[string]any{
				{
					"foo": "bar",
				},
				{
					"abc": "xyz",
				},
			},
		}
	}

	size := GetApproxSize(rowsData)

	// Check if size is non-zero and seems plausible
	assert.NotZero(t, size, "Size should not be zero")
	assert.Greater(t, size, 1000, "Size should be reasonably large for the given data structure")
}
