package consumer

import (
	"fmt"
	"regexp"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/models/event"
)

var topicConfig = kafkalib.TopicConfig{
	Database:  "customer",
	TableName: "users",
	Schema:    "public",
	Topic:     "foo",
}

func (f *FlushTestSuite) TestMemoryBasic() {
	// Set up expectations for the SQL mock
	// First, expect the DESC TABLE query
	f.mockDB.ExpectQuery(regexp.QuoteMeta(`DESC TABLE "CUSTOMER"."PUBLIC"."FOO"`)).
		WillReturnRows(sqlmock.NewRows([]string{"name", "type", "kind", "null", "default", "primary key", "unique key", "check", "expression", "comment"}).
			AddRow("id", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
			AddRow("abc", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
			AddRow("hi", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
			AddRow("__artie_delete", "BOOLEAN", "COLUMN", "YES", "", "NO", "NO", "", "", "").
			AddRow("__artie_only_set_delete", "BOOLEAN", "COLUMN", "YES", "", "NO", "NO", "", "", ""))

	// Then expect the INSERT queries
	for i := 0; i < 5; i++ {
		f.mockDB.ExpectExec(regexp.QuoteMeta(`INSERT INTO "CUSTOMER"."PUBLIC"."FOO"`)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	// Expect CREATE TABLE and DROP TABLE operations for temporary tables
	// These are generated with random suffixes, so we use regexp to match them
	createTableRegex := regexp.QuoteMeta(`CREATE TABLE IF NOT EXISTS "CUSTOMER"."PUBLIC"."FOO___ARTIE_`) + `.*` + regexp.QuoteMeta(`" ("ID" string,"ABC" string,"HI" string,"__ARTIE_DELETE" boolean,"__ARTIE_ONLY_SET_DELETE" boolean) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`)
	f.mockDB.ExpectExec(createTableRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Expect PUT command for staging
	putQueryRegex := regexp.QuoteMeta(`PUT 'file://`) + `.*` + regexp.QuoteMeta(`' @"CUSTOMER"."PUBLIC"."%FOO___ARTIE_`) + `.*` + regexp.QuoteMeta(`" AUTO_COMPRESS=TRUE`)
	f.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Expect COPY INTO command
	copyQueryRegex := regexp.QuoteMeta(`COPY INTO "CUSTOMER"."PUBLIC"."FOO___ARTIE_`) + `.*` + regexp.QuoteMeta(`" ("ID","ABC","HI","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE") FROM (SELECT $1,$2,$3,$4,$5 FROM @"CUSTOMER"."PUBLIC"."%FOO___ARTIE_`) + `.*` + regexp.QuoteMeta(`.csv.gz')`)
	f.mockDB.ExpectQuery(copyQueryRegex).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow("5"))

	// Expect DROP TABLE command
	dropQueryRegex := regexp.QuoteMeta(`DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."FOO___ARTIE_`) + `.*` + regexp.QuoteMeta(`"`)
	f.mockDB.ExpectExec(dropQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns("foo")

	for i := 0; i < 5; i++ {
		mockEvent.GetDataReturns(map[string]any{
			"id":                                fmt.Sprintf("pk-%d", i),
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			"abc":                               "def",
			"hi":                                "hello",
		}, nil)

		evt, err := event.ToMemoryEvent(mockEvent, map[string]any{"id": fmt.Sprintf("pk-%d", i)}, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(f.T(), err)

		kafkaMsg := kafka.Message{Partition: 1, Offset: 1}

		_, _, err = evt.Save(f.cfg, f.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
		assert.Nil(f.T(), err)

		td := f.db.GetOrCreateTableData("foo")
		assert.Equal(f.T(), int(td.NumberOfRows()), i+1)
	}

	assert.Equal(f.T(), uint(5), f.db.GetOrCreateTableData("foo").NumberOfRows())

	// Verify that all expectations were met
	assert.NoError(f.T(), f.mockDB.ExpectationsWereMet())
}

func (f *FlushTestSuite) TestShouldFlush() {
	var flush bool
	var flushReason string

	// Set up expectations for the SQL mock
	// First, expect the DESC TABLE query
	f.mockDB.ExpectQuery("DESC TABLE \"CUSTOMER\".\"PUBLIC\".\"POSTGRES\"").
		WillReturnRows(sqlmock.NewRows([]string{"name", "type", "kind", "null", "default", "primary key", "unique key", "check", "expression", "comment"}).
			AddRow("id", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
			AddRow("pk", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
			AddRow("foo", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
			AddRow("cat", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
			AddRow("__artie_delete", "BOOLEAN", "COLUMN", "YES", "", "NO", "NO", "", "", "").
			AddRow("__artie_only_set_delete", "BOOLEAN", "COLUMN", "YES", "", "NO", "NO", "", "", ""))

	// Then expect the INSERT queries
	for i := 0; i < int(float64(f.cfg.BufferRows)*1.5); i++ {
		f.mockDB.ExpectExec("INSERT INTO \"CUSTOMER\".\"PUBLIC\".\"POSTGRES\"").
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	// Expect CREATE TABLE and DROP TABLE operations for temporary tables
	// These are generated with random suffixes, so we use regexp to match them
	createTableRegex := regexp.QuoteMeta(`CREATE TABLE IF NOT EXISTS "CUSTOMER"."PUBLIC"."POSTGRES___ARTIE_`) + `.*` + regexp.QuoteMeta(`" ("ID" string,"PK" string,"FOO" string,"CAT" string,"__ARTIE_DELETE" boolean,"__ARTIE_ONLY_SET_DELETE" boolean) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`)
	f.mockDB.ExpectExec(createTableRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Expect PUT command for staging
	putQueryRegex := regexp.QuoteMeta(`PUT 'file://`) + `.*` + regexp.QuoteMeta(`' @"CUSTOMER"."PUBLIC"."%POSTGRES___ARTIE_`) + `.*` + regexp.QuoteMeta(`" AUTO_COMPRESS=TRUE`)
	f.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Expect COPY INTO command
	copyQueryRegex := regexp.QuoteMeta(`COPY INTO "CUSTOMER"."PUBLIC"."POSTGRES___ARTIE_`) + `.*` + regexp.QuoteMeta(`" ("ID","PK","FOO","CAT","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE") FROM (SELECT $1,$2,$3,$4,$5,$6 FROM @"CUSTOMER"."PUBLIC"."%POSTGRES___ARTIE_`) + `.*` + regexp.QuoteMeta(`") FILES = ('CUSTOMER.PUBLIC.POSTGRES___ARTIE_`) + `.*` + regexp.QuoteMeta(`.csv.gz')`)
	f.mockDB.ExpectQuery(copyQueryRegex).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow(fmt.Sprintf("%d", int(float64(f.cfg.BufferRows)*1.5))))

	// Expect DROP TABLE command
	dropQueryRegex := regexp.QuoteMeta(`DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."POSTGRES___ARTIE_`) + `.*` + regexp.QuoteMeta(`"`)
	f.mockDB.ExpectExec(dropQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	for i := 0; i < int(float64(f.cfg.BufferRows)*1.5); i++ {
		mockEvent := &mocks.FakeEvent{}
		mockEvent.GetTableNameReturns("postgres")
		mockEvent.GetDataReturns(map[string]any{
			"id":                                fmt.Sprintf("pk-%d", i),
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			"pk":                                fmt.Sprintf("pk-%d", i),
			"foo":                               "bar",
			"cat":                               "dog",
		}, nil)

		evt, err := event.ToMemoryEvent(mockEvent, map[string]any{"id": fmt.Sprintf("pk-%d", i)}, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(f.T(), err)

		kafkaMsg := kafka.Message{Partition: 1, Offset: int64(i)}
		flush, flushReason, err = evt.Save(f.cfg, f.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
		assert.Nil(f.T(), err)

		if flush {
			break
		}
	}

	assert.Equal(f.T(), "rows", flushReason)
	assert.True(f.T(), flush, "Flush successfully triggered via pool size.")

	// Verify that all expectations were met
	err := f.mockDB.ExpectationsWereMet()
	assert.NoError(f.T(), err)
}

// func (f *FlushTestSuite) TestMemoryConcurrency() {
// 	tableNames := []string{"dusty", "snowflake", "postgres"}
// 	var wg sync.WaitGroup

// 	// Set up expectations for the SQL mockg88
// 	// First, expect the DESC TABLE queries for each table
// 	for _, tableName := range tableNames {
// 		upperTableName := strings.ToUpper(tableName)
// 		f.mockDB.ExpectQuery("DESC TABLE \"CUSTOMER\".\"PUBLIC\".\"" + upperTableName + "\"").
// 			WillReturnRows(sqlmock.NewRows([]string{"name", "type", "kind", "null", "default", "primary key", "unique key", "check", "expression", "comment"}).
// 				AddRow("id", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
// 				AddRow("pk", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
// 				AddRow("foo", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
// 				AddRow("cat", "VARCHAR", "COLUMN", "YES", "", "NO", "NO", "", "", "").
// 				AddRow("__artie_delete", "BOOLEAN", "COLUMN", "YES", "", "NO", "NO", "", "", "").
// 				AddRow("__artie_only_set_delete", "BOOLEAN", "COLUMN", "YES", "", "NO", "NO", "", "", ""))
// 	}

// 	// Then expect the INSERT queries for each table
// 	for _, tableName := range tableNames {
// 		upperTableName := strings.ToUpper(tableName)
// 		for i := 0; i < 5; i++ {
// 			f.mockDB.ExpectExec("INSERT INTO \"CUSTOMER\".\"PUBLIC\".\"" + upperTableName + "\"").
// 				WillReturnResult(sqlmock.NewResult(1, 1))
// 		}
// 	}

// 	// Expect CREATE TABLE and DROP TABLE operations for temporary tables
// 	// These are generated with random suffixes, so we use regexp to match them
// 	for _, tableName := range tableNames {
// 		upperTableName := strings.ToUpper(tableName)
// 		// Allow multiple CREATE and DROP operations for each table
// 		for i := 0; i < 3; i++ {
// 			createTableRegex := regexp.QuoteMeta(`CREATE TABLE IF NOT EXISTS "CUSTOMER"."PUBLIC"."`) + upperTableName + regexp.QuoteMeta(`___ARTIE_`) + `.*` + regexp.QuoteMeta(`" ("ID" string,"PK" string,"FOO" string,"CAT" string,"__ARTIE_DELETE" boolean,"__ARTIE_ONLY_SET_DELETE" boolean) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`)
// 			f.mockDB.ExpectExec(createTableRegex).WillReturnResult(sqlmock.NewResult(0, 0))

// 			// Expect PUT command for staging
// 			putQueryRegex := regexp.QuoteMeta(`PUT 'file://`) + `.*` + regexp.QuoteMeta(`' @"CUSTOMER"."PUBLIC"."%`) + upperTableName + regexp.QuoteMeta(`___ARTIE_`) + `.*` + regexp.QuoteMeta(`" AUTO_COMPRESS=TRUE`)
// 			f.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

// 			// Expect COPY INTO command
// 			copyQueryRegex := regexp.QuoteMeta(`COPY INTO "CUSTOMER"."PUBLIC"."`) + upperTableName + regexp.QuoteMeta(`___ARTIE_`) + `.*` + regexp.QuoteMeta(`" ("ID","PK","FOO","CAT","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE") FROM (SELECT $1,$2,$3,$4,$5,$6 FROM @"CUSTOMER"."PUBLIC"."%`) + upperTableName + regexp.QuoteMeta(`___ARTIE_`) + `.*` + regexp.QuoteMeta(`") FILES = ('CUSTOMER.PUBLIC.`) + upperTableName + regexp.QuoteMeta(`___ARTIE_`) + `.*` + regexp.QuoteMeta(`.csv.gz')`)
// 			f.mockDB.ExpectQuery(copyQueryRegex).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow("5"))

// 			// Expect DROP TABLE command
// 			dropQueryRegex := regexp.QuoteMeta(`DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."`) + upperTableName + regexp.QuoteMeta(`___ARTIE_`) + `.*` + regexp.QuoteMeta(`"`)
// 			f.mockDB.ExpectExec(dropQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))
// 		}
// 	}

// 	// Create a channel to signal completion
// 	done := make(chan struct{})

// 	// Inserted a bunch of data
// 	for idx := range tableNames {
// 		wg.Add(1)
// 		go func(tableName string) {
// 			defer wg.Done()
// 			for i := 0; i < 5; i++ {
// 				mockEvent := &mocks.FakeEvent{}
// 				mockEvent.GetTableNameReturns(tableName)
// 				mockEvent.GetDataReturns(map[string]any{
// 					"id":                                fmt.Sprintf("pk-%d", i),
// 					constants.DeleteColumnMarker:        true,
// 					constants.OnlySetDeleteColumnMarker: true,
// 					"pk":                                fmt.Sprintf("pk-%d", i),
// 					"foo":                               "bar",
// 					"cat":                               "dog",
// 				}, nil)

// 				evt, err := event.ToMemoryEvent(mockEvent, map[string]any{"id": fmt.Sprintf("pk-%d", i)}, kafkalib.TopicConfig{}, config.Replication)
// 				assert.NoError(f.T(), err)

// 				kafkaMsg := kafka.Message{Partition: 1, Offset: int64(i)}
// 				_, _, err = evt.Save(f.cfg, f.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
// 				assert.Nil(f.T(), err)
// 			}
// 		}(tableNames[idx])
// 	}

// 	// Wait for all goroutines to complete
// 	wg.Wait()

// 	// Verify all the tables exist.
// 	for idx := range tableNames {
// 		td := f.db.GetOrCreateTableData(tableNames[idx])
// 		assert.Len(f.T(), td.Rows(), 5)
// 	}

// 	// Run the flush in a goroutine with a timeout
// 	go func() {
// 		err := Flush(f.T().Context(), f.db, f.dest, metrics.NullMetricsProvider{}, Args{})
// 		assert.Nil(f.T(), err, "flush failed")
// 		close(done)
// 	}()

// 	// Wait for the flush to complete or timeout
// 	select {
// 	case <-done:
// 		// Flush completed successfully
// 	case <-time.After(5 * time.Second):
// 		f.T().Fatal("Test timed out after 5 seconds")
// 	}

// 	assert.Equal(f.T(), f.fakeConsumer.CommitMessagesCallCount(), len(tableNames)) // Commit 3 times because 3 topics.

// 	for i := 0; i < len(tableNames); i++ {
// 		_, kafkaMessages := f.fakeConsumer.CommitMessagesArgsForCall(i)
// 		assert.Equal(f.T(), len(kafkaMessages), 1) // There's only 1 partition right now

// 		// Within each partition, the offset should be 4 (i < 5 from above).
// 		assert.Equal(f.T(), kafkaMessages[0].Offset, int64(4))
// 	}

// 	// Verify that all expectations were met
// 	err := f.mockDB.ExpectationsWereMet()
// 	assert.NoError(f.T(), err)
// }
