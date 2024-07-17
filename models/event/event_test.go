package event

import (
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type fakeEvent struct{}

var idMap = map[string]any{
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

func (f fakeEvent) GetColumns() (*columns.Columns, error) {
	return &columns.Columns{}, nil
}

func (f fakeEvent) GetData(pkMap map[string]any, config *kafkalib.TopicConfig) (map[string]any, error) {
	return map[string]any{constants.DeleteColumnMarker: false, constants.OnlySetDeleteColumnMarker: false}, nil
}

func (e *EventsTestSuite) TestEvent_IsValid() {
	{
		_evt := Event{
			Table: "foo",
		}
		assert.False(e.T(), _evt.IsValid())
	}
	{
		_evt := Event{
			Table:         "foo",
			PrimaryKeyMap: idMap,
		}
		assert.False(e.T(), _evt.IsValid())
	}
	{
		_evt := Event{
			Table:         "foo",
			PrimaryKeyMap: idMap,
			Data: map[string]any{
				"foo": "bar",
			},
			mode: config.History,
		}
		assert.True(e.T(), _evt.IsValid())
	}
	{
		_evt := Event{
			Table:         "foo",
			PrimaryKeyMap: idMap,
			Data: map[string]any{
				"foo": "bar",
			},
		}
		assert.False(e.T(), _evt.IsValid())
	}
	{
		_evt := Event{
			Table:         "foo",
			PrimaryKeyMap: idMap,
			Data:          map[string]any{constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true},
		}
		assert.True(e.T(), _evt.IsValid())
	}
}

func (e *EventsTestSuite) TestEvent_TableName() {
	var f fakeEvent
	{
		// Don't pass in tableName.
		evt, err := ToMemoryEvent(f, idMap, &kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), f.GetTableName(), evt.Table)
	}
	{
		// Now pass it in, it should override.
		evt, err := ToMemoryEvent(f, idMap, &kafkalib.TopicConfig{TableName: "orders"}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "orders", evt.Table)
	}
	{
		// Now, if it's history mode...
		evt, err := ToMemoryEvent(f, idMap, &kafkalib.TopicConfig{TableName: "orders"}, config.History)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "orders__history", evt.Table)

		// Table already has history suffix, so it won't add extra.
		evt, err = ToMemoryEvent(f, idMap, &kafkalib.TopicConfig{TableName: "dusty__history"}, config.History)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "dusty__history", evt.Table)
	}
}

func (e *EventsTestSuite) TestEvent_Columns() {
	var f fakeEvent
	{
		evt, err := ToMemoryEvent(f, map[string]any{"id": 123}, &kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)

		assert.Equal(e.T(), 1, len(evt.Columns.GetColumns()))
		_, isOk := evt.Columns.GetColumn("id")
		assert.True(e.T(), isOk)
	}
	{
		// Now it should handle escaping column names
		evt, err := ToMemoryEvent(f, map[string]any{"id": 123, "CAPITAL": "foo"}, &kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)

		assert.Equal(e.T(), 2, len(evt.Columns.GetColumns()))
		_, isOk := evt.Columns.GetColumn("id")
		assert.True(e.T(), isOk)

		_, isOk = evt.Columns.GetColumn("capital")
		assert.True(e.T(), isOk)
	}
	{
		// In history mode, the deletion column markers should be removed from the event data
		evt, err := ToMemoryEvent(f, map[string]any{"id": 123}, &kafkalib.TopicConfig{}, config.History)
		assert.NoError(e.T(), err)

		_, isOk := evt.Data[constants.DeleteColumnMarker]
		assert.False(e.T(), isOk)
		_, isOk = evt.Data[constants.OnlySetDeleteColumnMarker]
		assert.False(e.T(), isOk)
	}
}

func (e *EventsTestSuite) TestEventPrimaryKeys() {
	evt := &Event{
		Table: "foo",
		PrimaryKeyMap: map[string]any{
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
		PrimaryKeyMap: map[string]any{
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
		PrimaryKeyMap: map[string]any{
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
