package models

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/stretchr/testify/assert"
)

func (m *ModelsTestSuite) TestEvent_IsValid() {
	var e Event
	assert.False(m.T(), e.IsValid())

	e.Table = "foo"
	assert.False(m.T(), e.IsValid())

	e.PrimaryKeyName = "id"
	assert.False(m.T(), e.IsValid())

	e.PrimaryKeyValue = 123
	assert.False(m.T(), e.IsValid())

	e.Data = make(map[string]interface{})
	e.Data[config.DeleteColumnMarker] = false
	assert.True(m.T(), e.IsValid(), e)
}
