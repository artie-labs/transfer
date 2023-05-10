package models

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/artie-labs/transfer/lib/logger"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
)

const dbKey = "__db"

type DatabaseData struct {
	TableData map[string]*optimization.TableData
	sync.Mutex
}

func LoadMemoryDB(ctx context.Context) context.Context {
	return context.WithValue(ctx, dbKey, &DatabaseData{
		TableData: map[string]*optimization.TableData{},
	})
}

func GetMemoryDB(ctx context.Context) *DatabaseData {
	dbValue := ctx.Value(dbKey)
	if dbValue == nil {
		logger.FromContext(ctx).Fatalf("failed to retrieve database from context")
	}

	db, isOk := dbValue.(*DatabaseData)
	if !isOk {
		logger.FromContext(ctx).Fatalf("database data is not the right type *DatabaseData")
	}

	return db
}

func (d *DatabaseData) ClearTableConfig(tableName string) {
	// WARNING: before you call this, LOCK the table.
	delete(d.TableData, tableName)
}

// Save will save the event into our in memory event
// It will return two values, a boolean and error
// The boolean signifies whether we should flush immediately or not. This is because Snowflake has a constraint
// On the number of elements within an expression.
// The other, error - is returned to see if anything went awry.
func (e *Event) Save(ctx context.Context, topicConfig *kafkalib.TopicConfig, message artie.Message) (bool, error) {
	if topicConfig == nil {
		return false, errors.New("topicConfig is missing")
	}

	inMemDB := GetMemoryDB(ctx)

	inMemDB.Lock()
	defer inMemDB.Unlock()

	if !e.IsValid() {
		return false, errors.New("event not valid")
	}

	// Does the table exist?
	_, isOk := inMemDB.TableData[e.Table]
	if !isOk {
		columns := &typing.Columns{}
		if e.Columns != nil {
			columns = e.Columns
		}

		inMemDB.TableData[e.Table] = optimization.NewTableData(columns, e.PrimaryKeys(), *topicConfig)
	} else {
		if e.Columns != nil {
			// Iterate over this again just in case.
			for _, col := range e.Columns.GetColumns() {
				inMemDB.TableData[e.Table].InMemoryColumns.AddColumn(col)
			}
		}
	}

	// Update col if necessary
	sanitizedData := make(map[string]interface{})
	for _col, val := range e.Data {
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

		if val == "__debezium_unavailable_value" {
			// This is an edge case within Postgres & ORCL
			// TL;DR - Sometimes a column that is unchanged within a DML will not be emitted
			// DBZ has stubbed it out by providing this value, so we will skip it when we see it.
			// See: https://issues.redhat.com/browse/DBZ-4276
			// We are directly adding this column to our in-memory database
			// This ensures that this column exists, we just have an invalid value (so we will not replicate over).
			// However, this will ensure that we do not drop the column within the destination
			inMemDB.TableData[e.Table].InMemoryColumns.AddColumn(typing.Column{
				Name:        newColName,
				KindDetails: typing.Invalid,
			})
			continue
		}

		retrievedColumn, isOk := inMemDB.TableData[e.Table].InMemoryColumns.GetColumn(newColName)
		if !isOk {
			// This would only happen if the columns did not get passed in initially.
			inMemDB.TableData[e.Table].InMemoryColumns.AddColumn(typing.Column{
				Name:        newColName,
				KindDetails: typing.ParseValue(_col, e.OptiomalSchema, val),
			})
		} else {
			if retrievedColumn.KindDetails == typing.Invalid {
				// If colType is Invalid, let's see if we can update it to a better type
				// If everything is nil, we don't need to add a column
				// However, it's important to create a column even if it's nil.
				// This is because we don't want to think that it's okay to drop a column in DWH
				if kindDetails := typing.ParseValue(_col, e.OptiomalSchema, val); kindDetails.Kind != typing.Invalid.Kind {
					inMemDB.TableData[e.Table].InMemoryColumns.UpdateColumn(typing.Column{
						Name:        newColName,
						KindDetails: kindDetails,
					})
				}
			}
		}

		sanitizedData[newColName] = val
	}

	// Swap out sanitizedData <> data.
	e.Data = sanitizedData
	inMemDB.TableData[e.Table].InsertRow(e.PrimaryKeyValue(), e.Data)
	// If the message is Kafka, then we only need the latest one
	// If it's pubsub, we will store all of them in memory. This is because GCP pub/sub REQUIRES us to ack every single message
	if message.Kind() == artie.Kafka {
		inMemDB.TableData[e.Table].PartitionsToLastMessage[message.Partition()] = []artie.Message{message}
	} else {
		inMemDB.TableData[e.Table].PartitionsToLastMessage[message.Partition()] = append(inMemDB.TableData[e.Table].PartitionsToLastMessage[message.Partition()], message)
	}

	inMemDB.TableData[e.Table].LatestCDCTs = e.ExecutionTime
	return inMemDB.TableData[e.Table].ShouldFlush(ctx), nil
}
