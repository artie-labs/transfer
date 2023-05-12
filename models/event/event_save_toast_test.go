package event

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/models"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
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
	for _, col := range []string{"id", "first_name", "last_name", "email"} {
		cols.AddColumn(typing.Column{
			Name:        col,
			KindDetails: typing.Invalid,
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

func (e *EventsTestSuite) TestEventSave_Toast() {
	testCases := []testCase{
		// 1) Toast followed by normal value, same userId
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  "Tang",
					}),
				},
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": "Robin",
						"last_name":  "Tang",
					}),
					shouldFlush:     true,
					shouldReprocess: true,
				},
			},
		},
		// 2) Normal value followed by toast same userId
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": "Robin",
						"last_name":  "Tang",
					}),
				},
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  "Tang",
					}),
					shouldFlush:     true,
					shouldReprocess: true,
				},
			},
		},
		// 3) Toast followed by Toast same userId
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  "Tang1",
					}),
				},
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  "Tang2",
					}),
				},
			},
		},
		// 4) Normal followed by normal same userId
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": "Robin",
						"last_name":  "Tang1",
					}),
				},
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": "Tangster",
						"last_name":  "Tang2",
					}),
				},
			},
		},
		// 5) Toast followed normal value, different userIds
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  "Tang1",
					}),
				},
				{
					Event: newTestEvent(123, map[string]interface{}{
						"first_name": "Tangster",
						"last_name":  "Tang2",
					}),
					shouldReprocess: true,
					shouldFlush:     true,
				},
			},
		},
		// 6) Normal value followed by toast value, different userIds
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": "Tangster",
						"last_name":  "Tang1",
					}),
				},
				{
					Event: newTestEvent(123, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  "Tang2",
					}),
					shouldReprocess: true,
					shouldFlush:     true,
				},
			},
		},
		// 7) Toast followed by Toast different userIds
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  "Tang1",
					}),
				},
				{
					Event: newTestEvent(123, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  "Tang2",
					}),
				},
			},
		},
		// 8) Normal followed by normal different userIds
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": "Robin2",
						"last_name":  "Tang1",
					}),
				},
				{
					Event: newTestEvent(123, map[string]interface{}{
						"first_name": "Robin1",
						"last_name":  "Tang2",
					}),
				},
			},
		},
		// 9) Higher number of toasts mixed with normal rows
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  debezium.ToastUnavailableValuePlaceholder,
					}),
				},
				{
					Event: newTestEvent(123, map[string]interface{}{
						"first_name": "Robin1",
						"last_name":  "Tang2",
					}),
					shouldFlush:     true,
					shouldReprocess: true,
				},
			},
		},
		// 10) Mixed toast values with different user id
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  debezium.ToastUnavailableValuePlaceholder,
						"email":      "robin@artie.so",
					}),
				},
				{
					Event: newTestEvent(123, map[string]interface{}{
						"first_name": "Robin1",
						"last_name":  "Tang",
						"email":      "robin@artie.so",
					}),
					shouldFlush:     true,
					shouldReprocess: true,
				},
			},
		},
		// 11) Multiple toast columns, but all the same.
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  debezium.ToastUnavailableValuePlaceholder,
						"email":      "robin@artie.so",
					}),
				},
				{
					Event: newTestEvent(123, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  debezium.ToastUnavailableValuePlaceholder,
						"email":      "jacqueline@artie.so",
					}),
				},
			},
		},
		// 12) Multiple toast columns, same, but followed by normal.
		{
			events: []flushEvent{
				{
					Event: newTestEvent(1, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  debezium.ToastUnavailableValuePlaceholder,
						"email":      "robin@artie.so",
					}),
				},
				{
					Event: newTestEvent(123, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  debezium.ToastUnavailableValuePlaceholder,
						"email":      "jacqueline@artie.so",
					}),
				},
				{
					Event: newTestEvent(1234, map[string]interface{}{
						"first_name": debezium.ToastUnavailableValuePlaceholder,
						"last_name":  "Young",
						"email":      "charlie@artie.so",
					}),
					shouldReprocess: true,
					shouldFlush:     true,
				},
			},
		},
	}

	for _, test := range testCases {
		// Flush after each test case.
		e.ctx = models.LoadMemoryDB(e.ctx)
		for _, evt := range test.events {
			kafkaMsg := kafka.Message{}
			flush, reprocess, err := evt.Event.Save(e.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
			assert.Equal(e.T(), evt.shouldFlush, flush, fmt.Sprintf("evt_data: %v, flush is %v", evt.Event.Data, flush))
			assert.Equal(e.T(), evt.shouldReprocess, reprocess, fmt.Sprintf("evt_data: %v, reprocess is %v", evt.Event.Data, reprocess))
			assert.NoError(e.T(), err)
		}

	}

}
