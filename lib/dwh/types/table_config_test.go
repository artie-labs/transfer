package types

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDwhTableConfig_ShouldDeleteColumn(t *testing.T) {
	dwhTableConfig := NewDwhTableConfig(nil, nil, false, false)
	results := dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC())
	assert.False(t, results)
	assert.Equal(t, len(dwhTableConfig.ColumnsToDelete()), 0)

	// Once the flag is turned on.
	dwhTableConfig.dropDeletedColumns = true
	results = dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC())
	assert.False(t, results)
	assert.Equal(t, len(dwhTableConfig.ColumnsToDelete()), 1)
}
