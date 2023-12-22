package size

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVariableToBytes(t *testing.T) {
	filePath := "/tmp/size_test"
	assert.NoError(t, os.RemoveAll(filePath))

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

	err := os.WriteFile(filePath, []byte(fmt.Sprint(rowsData)), os.ModePerm)
	assert.NoError(t, err)

	stat, err := os.Stat(filePath)
	assert.NoError(t, err)

	size := GetApproxSize(rowsData)
	assert.Equal(t, int(stat.Size()), size)
}
