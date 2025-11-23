package event

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (e *EventsTestSuite) TestEvent_Validate() {
	{
		_evt := Event{table: "foo"}
		assert.ErrorContains(e.T(), _evt.Validate(), "primary keys are empty")
	}
	{
		_evt := Event{table: "foo", primaryKeys: []string{"id"}}
		assert.ErrorContains(e.T(), _evt.Validate(), "event has no data")
	}
	{
		_evt := Event{
			table:       "foo",
			primaryKeys: []string{"id"},
			data: map[string]any{
				"id":  123,
				"foo": "bar",
			},
			mode: config.History,
		}
		assert.NoError(e.T(), _evt.Validate())
	}
	{
		_evt := Event{
			table:       "foo",
			primaryKeys: []string{"id"},
			data: map[string]any{
				"id":  123,
				"foo": "bar",
			},
		}
		assert.ErrorContains(e.T(), _evt.Validate(), "delete column marker does not exist")
	}
	{
		_evt := Event{
			table:       "foo",
			primaryKeys: []string{"id"},
			data: map[string]any{
				"id":                                123,
				constants.DeleteColumnMarker:        true,
				constants.OnlySetDeleteColumnMarker: true,
			},
		}
		assert.NoError(e.T(), _evt.Validate())
	}
}

func testBuildFilteredColumns(t *testing.T, fakeEvent *mocks.FakeEvent, topicConfig kafkalib.TopicConfig, fakeColumns []columns.Column, expectedCols *columns.Columns) {
	fakeEvent.GetColumnsReturns(columns.NewColumns(fakeColumns), nil)

	cols, err := buildColumns(fakeEvent, topicConfig, nil)
	assert.NoError(t, err)
	assert.Equal(t, expectedCols.GetColumns(), cols.GetColumns())
}

func (e *EventsTestSuite) TestBuildFilteredColumns() {
	{
		// Not excluding or including anything
		fakeCols := []columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
		}
		testBuildFilteredColumns(e.T(), e.fakeEvent, kafkalib.TopicConfig{}, fakeCols, columns.NewColumns(fakeCols))
	}
	{
		// Exclude foo
		fakeCols := []columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
		}
		testBuildFilteredColumns(e.T(), e.fakeEvent, kafkalib.TopicConfig{ColumnsToExclude: []string{"foo"}}, fakeCols, columns.NewColumns([]columns.Column{
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
		}))
	}
	{
		// Include foo
		fakeCols := []columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
		}
		testBuildFilteredColumns(e.T(), e.fakeEvent, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}}, fakeCols, columns.NewColumns([]columns.Column{
			columns.NewColumn("foo", typing.String),
		}))
	}
	{
		// Include foo, but also artie columns
		fakeCols := []columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
			columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		}
		testBuildFilteredColumns(e.T(), e.fakeEvent, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}}, fakeCols, columns.NewColumns([]columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		}))
	}
}

func (e *EventsTestSuite) TestEvent_TableName() {
	id := []string{"id"}
	{
		// Don't pass in tableName.
		evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, id, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), e.fakeEvent.GetTableName(), evt.GetTable())
	}
	{
		// Now pass it in, it should override.
		evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, id, kafkalib.TopicConfig{TableName: "orders"}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "orders", evt.GetTable())
	}
	{
		// Now, if it's history mode...
		evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, id, kafkalib.TopicConfig{TableName: "orders"}, config.History)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "orders__history", evt.GetTable())

		// Table already has history suffix, so it won't add extra.
		evt, err = ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, id, kafkalib.TopicConfig{TableName: "dusty__history"}, config.History)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "dusty__history", evt.GetTable())
	}
}

func (e *EventsTestSuite) TestEvent_Columns() {
	id := []string{"id"}
	{
		evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, id, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)

		assert.Equal(e.T(), 1, len(evt.columns.GetColumns()))
		_, ok := evt.columns.GetColumn("id")
		assert.True(e.T(), ok)
	}
	{
		evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, id, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)

		assert.Equal(e.T(), 2, len(evt.columns.GetColumns()))
		_, ok := evt.columns.GetColumn("id")
		assert.True(e.T(), ok)

		_, ok = evt.columns.GetColumn("capital")
		assert.True(e.T(), ok)
	}
	{
		// In history mode, the deletion column markers should be removed from the event data
		evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, id, kafkalib.TopicConfig{}, config.History)
		assert.NoError(e.T(), err)

		_, ok := evt.data[constants.DeleteColumnMarker]
		assert.False(e.T(), ok)
		_, ok = evt.data[constants.OnlySetDeleteColumnMarker]
		assert.False(e.T(), ok)
	}
}

func (e *EventsTestSuite) TestEvent_PrimaryKeysOverride() {
	{
		// No primary keys override
		evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, []string{"not_id"}, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), []string{"not_id"}, evt.GetPrimaryKeys())
	}
	{
		// Specified primary keys override
		evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, []string{"not_id"}, kafkalib.TopicConfig{PrimaryKeysOverride: []string{"id"}}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), []string{"id"}, evt.GetPrimaryKeys())
	}
}

func (e *EventsTestSuite) TestEvent_StaticColumns() {
	{
		// Should error if there's a static column collision
		e.fakeEvent.GetDataReturns(map[string]any{"id": 123}, nil)
		_, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, []string{"id"}, kafkalib.TopicConfig{StaticColumns: []kafkalib.StaticColumn{{Name: "id", Value: "123"}}}, config.Replication)
		assert.ErrorContains(e.T(), err, `static column "id" collides with event data`)
	}
	{
		// No error since there's no collision
		e.fakeEvent.GetDataReturns(map[string]any{"id": 123}, nil)
		evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, e.fakeEvent, []string{"id"}, kafkalib.TopicConfig{StaticColumns: []kafkalib.StaticColumn{{Name: "foo", Value: "bar"}}}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), map[string]any{"id": 123, "foo": "bar"}, evt.data)
	}
}

func (e *EventsTestSuite) TestToMemoryEventWithSoftPartitioning() {
	partitionFrequencies := []kafkalib.PartitionFrequency{
		kafkalib.Monthly,
		kafkalib.Daily,
		kafkalib.Hourly,
	}
	createdAt, err := time.Parse("2006-01-02T15:04:05Z", "2024-06-01T12:34:56Z")
	assert.NoError(e.T(), err)

	for _, freq := range partitionFrequencies {
		softPartitioning := kafkalib.SoftPartitioning{
			Enabled:            true,
			PartitionColumn:    "created_at",
			PartitionFrequency: freq,
			PartitionSchema:    "soft_part_schema",
		}
		tc := kafkalib.TopicConfig{
			Database:         "customer",
			TableName:        "users",
			Schema:           "public",
			SoftPartitioning: softPartitioning,
			Topic:            "customer.public.users",
		}

		mockEvent := &mocks.FakeEvent{}
		mockEvent.GetTableNameReturns(tc.TableName)
		mockEvent.GetDataReturns(map[string]any{
			"id":                                "123",
			"created_at":                        createdAt,
			constants.DeleteColumnMarker:        false,
			constants.OnlySetDeleteColumnMarker: false,
			"randomCol":                         "dusty",
		}, nil)
		mockEvent.GetOptionalSchemaReturns(map[string]typing.KindDetails{
			"created_at": typing.Time,
		}, nil)

		event, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, mockEvent, []string{"id"}, tc, config.Replication)
		assert.NoError(e.T(), err)

		// Verify that the event has the correct partitioned table name
		suffix, err := softPartitioning.PartitionFrequency.Suffix(createdAt)
		assert.NoError(e.T(), err)
		expectedTableName := tc.TableName + suffix
		assert.Equal(e.T(), expectedTableName, event.GetTable(), "Table name should include partition suffix for frequency %s", freq)

		// Verify that the event data contains the expected fields
		assert.Equal(e.T(), "123", event.data["id"])
		assert.Equal(e.T(), createdAt, event.data["created_at"])
		assert.Equal(e.T(), "dusty", event.data["randomCol"])
		assert.Equal(e.T(), false, event.data[constants.DeleteColumnMarker])
		assert.Equal(e.T(), false, event.data[constants.OnlySetDeleteColumnMarker])

		// Verify primary keys
		assert.Equal(e.T(), []string{"id"}, event.GetPrimaryKeys())

		// Verify that the event has the correct table ID structure
		// Note: partition schema is not used for the table ID yet, using the schema from the topic config
		expectedTableID := fmt.Sprintf("%s.%s", tc.Schema, expectedTableName)
		assert.Equal(e.T(), expectedTableID, event.GetTableID().String())
	}
}
