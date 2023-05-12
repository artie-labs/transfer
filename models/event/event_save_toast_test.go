package event

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/segmentio/kafka-go"
)

type flushEvent struct {
	Event           Event
	shouldFlush     bool
	shouldReprocess bool
}

type testCase struct {
	events []flushEvent
}

func newTestEvent(pk int, data map[string]interface{}) Event {
	var cols typing.Columns
	for _, col := range []string{"id", "first_name", "last_name"} {
		cols.AddColumn(typing.Column{
			Name:        col,
			KindDetails: typing.String,
		})
	}

	data[constants.DeleteColumnMarker] = false
	data["id"] = pk

	return Event{
		Table:   "test_table",
		Columns: &cols,
		PrimaryKeyMap: map[string]interface{}{
			"id": fmt.Sprint(pk),
		},
		Data: data,
	}
}

func (e *EventsTestSuite) TestEventSave_ToastScenarioOne() {
	testCases := []testCase{
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": "Robin",
						"last_name":  "Tang",
					}),
				},
				{
					Event: newTestEvent(2, map[string]interface{}{
						"first_name": "Robin",
						"last_name":  "Tang",
					}),
				},
			},
		},
	}

	for _, test := range testCases {
		kafkaMsg := kafka.Message{}

		for _, evt := range test.events {
			flush, reprocess, err := evt.Event.Save(e.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
			assert.Equal(e.T(), evt.shouldFlush, flush)
			assert.Equal(e.T(), evt.shouldReprocess, reprocess)
			assert.NoError(e.T(), err)
		}

	}

}
