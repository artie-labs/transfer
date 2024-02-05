package event

import (
	"time"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type fakeEvent struct{}

var idMap = map[string]interface{}{
	"id": 123,
}

func (f fakeEvent) Operation() string {
	return "r"
}

func (f fakeEvent) DeletePayload() bool {
	return false
}

func (f fakeEvent) GetExecutionTime() time.Time {
	return time.Now()
}

func (f fakeEvent) GetTableName() string {
	return "foo"
}

func (f fakeEvent) GetOptionalSchema() map[string]typing.KindDetails {
	return nil
}

func (f fakeEvent) GetColumns() *columns.Columns {
	return &columns.Columns{}
}

func (f fakeEvent) GetData(pkMap map[string]interface{}, config *kafkalib.TopicConfig) map[string]interface{} {
	return map[string]interface{}{constants.DeleteColumnMarker: false}
}

func (e *EventsTestSuite) TestEvent_IsValid() {
	var _evt Event
	assert.False(e.T(), _evt.IsValid())

	_evt.Table = "foo"
	assert.False(e.T(), _evt.IsValid())

	_evt.PrimaryKeyMap = idMap
	assert.False(e.T(), _evt.IsValid())

	_evt.Data = make(map[string]interface{})
	_evt.Data[constants.DeleteColumnMarker] = false
	assert.True(e.T(), _evt.IsValid(), _evt)
}

func (e *EventsTestSuite) TestEvent_TableName() {
	var f fakeEvent
	{
		// Don't pass in tableName.
		evt := ToMemoryEvent(f, idMap, &kafkalib.TopicConfig{}, config.Replication)
		assert.Equal(e.T(), f.GetTableName(), evt.Table)
	}
	{
		// Now pass it in, it should override.
		evt := ToMemoryEvent(f, idMap, &kafkalib.TopicConfig{
			TableName: "orders",
		}, config.Replication)
		assert.Equal(e.T(), "orders", evt.Table)
	}
	{
		// Now, if it's history mode...
		evt := ToMemoryEvent(f, idMap, &kafkalib.TopicConfig{
			TableName: "orders",
		}, config.History)
		assert.Equal(e.T(), "orders__history", evt.Table)
	}
}

func (e *EventsTestSuite) TestEventPrimaryKeys() {
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

		assert.True(e.T(), found, requiredKey)
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

	assert.True(e.T(), found, anotherEvt.PrimaryKeyValue())

	// Make sure the ordering for the pk is deterministic.
	partsMap := make(map[string]bool)
	for i := 0; i < 100; i++ {
		partsMap[anotherEvt.PrimaryKeyValue()] = true
	}

	assert.Equal(e.T(), len(partsMap), 1)
}

func (e *EventsTestSuite) TestPrimaryKeyValueDeterministic() {
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
		assert.Equal(e.T(), evt.PrimaryKeyValue(), "aa=1bb=5dusty=mini aussiegg=artiezz=ff")
	}
}
