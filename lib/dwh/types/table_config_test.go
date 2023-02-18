package types

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDwhTableConfig_ShouldDeleteColumn(t *testing.T) {
	dwhTableConfig := NewDwhTableConfig(nil, nil, false)
	results := dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC())
	assert.False(t, results)
	assert.Equal(t, len(dwhTableConfig.ColumnsToDelete()), 0)

	dwhTableConfig.DropDeletedColumns = true
}
