package models

import (
	"context"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
	"time"
)

type fakeEvent struct{}

var idMap = map[string]interface{}{
	"id": 123,
}

func (f fakeEvent) GetExecutionTime() time.Time {
	return time.Now()
}

func (f fakeEvent) GetData(ctx context.Context, pkMap map[string]interface{}, config *kafkalib.TopicConfig) map[string]interface{} {
	return map[string]interface{}{constants.DeleteColumnMarker: false}
}

func (m *ModelsTestSuite) TestEvent_IsValid() {
	var e Event
	assert.False(m.T(), e.IsValid())

	e.Table = "foo"
	assert.False(m.T(), e.IsValid())

	e.PrimaryKeyMap = idMap
	assert.False(m.T(), e.IsValid())

	e.Data = make(map[string]interface{})
	e.Data[constants.DeleteColumnMarker] = false
	assert.True(m.T(), e.IsValid(), e)
}

func (m *ModelsTestSuite) TestEvent_TableName() {
	var f fakeEvent
	evt := ToMemoryEvent(context.Background(), f, idMap, &kafkalib.TopicConfig{
		TableName: "orders",
	})

	assert.Equal(m.T(), "orders", evt.Table)
}
