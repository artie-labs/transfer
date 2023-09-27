package models

import (
	"time"

	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/stretchr/testify/assert"
)

func (m *ModelsTestSuite) TestMergeOperations() {
	coolDown := 5 * time.Second
	checkInterval := 200 * time.Millisecond

	td := TableData{
		TableData: &optimization.TableData{},
	}

	// Before wiping, we should not skip the merge since ts did not get set yet.
	assert.False(m.T(), td.ShouldSkipMerge(coolDown))

	td.Wipe()
	for i := 0; i < 10; i++ {
		assert.True(m.T(), td.ShouldSkipMerge(coolDown))
		time.Sleep(checkInterval)
	}

	time.Sleep(3 * time.Second)
	assert.False(m.T(), td.ShouldSkipMerge(coolDown))
}
