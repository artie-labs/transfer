package size

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetApproxSize(t *testing.T) {
	rowsData := make(map[string]interface{}) // pk -> { col -> val }
	for i := 0; i < 500; i++ {
		rowsData[fmt.Sprintf("key-%v", i)] = map[string]interface{}{
			"id":         fmt.Sprintf("key-%v", i),
			"artie":      "transfer",
			"dusty":      "the mini aussie",
			"next_puppy": true,
			"team":       []string{"charlie", "robin", "jacqueline"},
		}
	}

	size := GetApproxSize(rowsData)

	// Check if size is non-zero and seems plausible
	assert.NotZero(t, size, "Size should not be zero")
	assert.Greater(t, size, 1000, "Size should be reasonably large for the given data structure")
}
