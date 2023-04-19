package models

import (
	"context"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
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

func (f fakeEvent) GetOptionalSchema(ctx  context.Context) map[string]typing.KindDetails {
	return nil
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

func (m *ModelsTestSuite) TestEventPrimaryKeys() {
	evt := &Event{
		Table: "foo",
		PrimaryKeyMap: map[string]interface{}{
			"id":  true,
			"id1": true,
			"id2": true,
			"id3": true,
			"id4": true,
		},
	}

	requiredKeys := []string{"id", "id1", "id2", "id3", "id4"}
	for _, requiredKey := range requiredKeys {
		var found bool
		for _, primaryKey := range evt.PrimaryKeys() {
			found = requiredKey == primaryKey
			if found {
				break
			}
		}

		assert.True(m.T(), found, requiredKey)
	}

	anotherEvt := &Event{
		Table: "foo",
		PrimaryKeyMap: map[string]interface{}{
			"id":        1,
			"course_id": 2,
		},
	}

	var found bool
	possibilities := []string{"course_id=2id=1"}
	pkVal := anotherEvt.PrimaryKeyValue()
	for _, possibility := range possibilities {
		if found = possibility == pkVal; found {
			break
		}
	}

	assert.True(m.T(), found, anotherEvt.PrimaryKeyValue())

	// Make sure the ordering for the pk is deterministic.
	partsMap := make(map[string]bool)
	for i := 0; i < 100; i++ {
		partsMap[anotherEvt.PrimaryKeyValue()] = true
	}

	assert.Equal(m.T(), len(partsMap), 1)
}

func (m *ModelsTestSuite) TestPrimaryKeyValueDeterministic() {
	evt := &Event{
		PrimaryKeyMap: map[string]interface{}{
			"aa":    1,
			"bb":    5,
			"zz":    "ff",
			"gg":    "artie",
			"dusty": "mini aussie",
		},
	}

	for i := 0; i < 500*1000; i++ {
		assert.Equal(m.T(), evt.PrimaryKeyValue(), "aa=1bb=5dusty=mini aussiegg=artiezz=ff")
	}
}
