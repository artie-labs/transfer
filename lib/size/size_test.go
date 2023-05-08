package size

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestVariableToBytes(t *testing.T) {
	filePath := "/tmp/size_test"
	assert.NoError(t, os.RemoveAll(filePath))

	rowsData := make(map[string]map[string]interface{}) // pk -> { col -> val }
	for i := 0; i < 500; i ++ {
		rowsData[fmt.Sprintf("key-%v", i)] = map[string]interface{}{
			"id": fmt.Sprintf("key-%v", i),
			"artie": "transfer",
			"dusty": "the mini aussie",
			"next_puppy": true,
			"team": []string{"charlie", "robin", "jacqueline"},
		}
	}

	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(rowsData)
	assert.NoError(t, err)

	err = os.WriteFile(filePath, b.Bytes(), os.ModePerm)
	assert.NoError(t, err)

	stat, err := os.Stat(filePath)
	assert.NoError(t, err)

	size, err := getRealSizeOf(rowsData)
	assert.NoError(t, err)
	assert.Equal(t, int(stat.Size()), size)

	// This file should be 75 kb, so let's test the limit.
	sizeToCrossedMap := map[int]bool{
		100: false,
		50: true,
		30: true,
	}

	for thresholdSize, crossed := range sizeToCrossedMap {
		actuallyCrossed, err := CrossedThreshold(rowsData, thresholdSize)
		assert.NoError(t, err)

		fmt.Println("crossed", crossed, "actuallyCrossed", actuallyCrossed, "size", size, "thresholdSize", thresholdSize)
		assert.Equal(t, crossed, actuallyCrossed)
	}
}
