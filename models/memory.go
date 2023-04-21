package models

import (
	"context"
	"errors"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/models/flush"
	"strings"
)

// Save will save the event into our in memory event
// It will return two values, a boolean and error
// The boolean signifies whether we should flush immediately or not. This is because Snowflake has a constraint
// On the number of elements within an expression.
// The other, error - is returned to see if anything went awry.
func (e *Event) Save(ctx context.Context, topicConfig *kafkalib.TopicConfig, message artie.Message) (bool, error) {
	if topicConfig == nil {
		return false, errors.New("topicConfig is missing")
	}

	db := flush.FromContext(ctx)
	if !e.IsValid() {
		return false, errors.New("event not valid")
	}

	table := db.GetTable(e.Table)
	// Does the table exist?
	if table == nil {
		table = db.NewTable(e.Table, e.PrimaryKeys(), topicConfig)
	}

	// Update col if necessary
	sanitizedData := make(map[string]interface{})
	for _col, val := range e.Data {
		// TODO test _col case sensitive operation.

		// columns need to all be normalized and lower cased.
		newColName := strings.ToLower(_col)

		// Columns here could contain spaces. Every destination treats spaces in a column differently.
		// So far, Snowflake accepts them when escaped properly, however BigQuery does not accept it.
		// Instead of making this more complicated for future destinations, we will escape the spaces by having double underscore `__`
		// So, if customers want to retrieve spaces again, they can replace `__`.
		var containsSpace bool
		containsSpace, newColName = stringutil.EscapeSpaces(newColName)
		if containsSpace {
			// Write the message back if the column has changed.
			sanitizedData[newColName] = val
		}

		// TODO: Test
		if val == "__debezium_unavailable_value" {
			// This is an edge case within Postgres & ORCL
			// TL;DR - Sometimes a column that is unchanged within a DML will not be emitted
			// DBZ has stubbed it out by providing this value, so we will skip it when we see it.
			// See: https://issues.redhat.com/browse/DBZ-4276
			// We are directly adding this column to our in-memory database
			// This ensures that this column exists, we just have an invalid value (so we will not replicate over).
			// However, this will ensure that we do not drop the column within the destination
			table.ModifyColumnType(newColName, typing.Invalid)
			continue
		}

		colTypeDetails, isOk := table.InMemoryColumns()[newColName]
		if !isOk {
			table.ModifyColumnType(newColName, typing.ParseValue(_col, e.OptiomalSchema, val))
		} else {
			if colTypeDetails.Kind == typing.Invalid.Kind {
				// If colType is Invalid, let's see if we can update it to a better type
				// If everything is nil, we don't need to add a column
				// However, it's important to create a column even if it's nil.
				// This is because we don't want to think that it's okay to drop a column in DWH
				if kindDetails := typing.ParseValue(_col, e.OptiomalSchema, val); kindDetails.Kind != typing.Invalid.Kind {
					table.ModifyColumnType(newColName, kindDetails)
				}
			}
		}

		sanitizedData[newColName] = val
	}

	// Swap out sanitizedData <> data.
	e.Data = sanitizedData
	table.AddRowData(e.PrimaryKeyValue(), e.Data)
	// If the message is Kafka, then we only need the latest one
	// If it's pubsub, we will store all of them in memory. This is because GCP pub/sub REQUIRES us to ack every single message

	table.UpdatePartitionsToLastMessage(message, e.ExecutionTime)

	settings := config.FromContext(ctx)
	return table.Rows() > settings.Config.BufferRows, nil
}
