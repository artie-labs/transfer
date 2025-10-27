package dialect

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestBigQueryDialect_QuoteIdentifier(t *testing.T) {
	dialect := BigQueryDialect{}
	assert.Equal(t, "`foo`", dialect.QuoteIdentifier("foo"))
	assert.Equal(t, "`FOO`", dialect.QuoteIdentifier("FOO"))
	assert.Equal(t, "`FOO; BAD`", dialect.QuoteIdentifier("FOO`; BAD"))
}

func TestBigQueryDialect_IsColumnAlreadyExistsErr(t *testing.T) {
	{
		// Random error
		assert.False(t, BigQueryDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf("hello there qux")))
	}
	{
		// Valid
		assert.True(t, BigQueryDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf("Column already exists")))
	}
}

func fromExpiresDateStringToTime(tsString string) (time.Time, error) {
	return time.Parse(bqLayout, tsString)
}

func TestBQExpiresDate(t *testing.T) {
	// We should be able to go back and forth.
	// Note: The format does not have ns precision because we don't need it.
	birthday := time.Date(2022, time.September, 6, 3, 19, 24, 0, time.UTC)
	for i := 0; i < 5; i++ {
		ts, err := fromExpiresDateStringToTime(BQExpiresDate(birthday))
		assert.NoError(t, err)
		assert.Equal(t, birthday, ts)
	}

	for _, badString := range []string{"foo", "bad_string", " 2022-09-01"} {
		_, err := fromExpiresDateStringToTime(badString)
		assert.ErrorContains(t, err, "cannot parse", badString)
	}
}

func TestBigQueryDialect_BuildCreateTableQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	// Temporary:
	assert.Contains(t,
		BigQueryDialect{}.BuildCreateTableQuery(fakeTableID, true, []string{"{PART_1}", "{PART_2}"}),
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2}) OPTIONS (expiration_timestamp = TIMESTAMP(`,
	)
	// Not temporary:
	assert.Equal(t,
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2})`,
		BigQueryDialect{}.BuildCreateTableQuery(fakeTableID, false, []string{"{PART_1}", "{PART_2}"}),
	)
}

func TestBigQueryDialect_BuildDropTableQuery(t *testing.T) {
	assert.Equal(t,
		"DROP TABLE IF EXISTS `project1`.`dataset2`.`table3`",
		BigQueryDialect{}.BuildDropTableQuery(NewTableIdentifier("project1", "dataset2", "table3")),
	)
}

func TestBigQueryDialect_BuildTruncateTableQuery(t *testing.T) {
	assert.Equal(t,
		"TRUNCATE TABLE `project1`.`dataset2`.`table3`",
		BigQueryDialect{}.BuildTruncateTableQuery(NewTableIdentifier("project1", "dataset2", "table3")),
	)
}

func TestBigQueryDialect_BuildDropColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} DROP COLUMN {SQL_PART}",
		BigQueryDialect{}.BuildDropColumnQuery(fakeTableID, "{SQL_PART}"),
	)
}

func TestBigQueryDialect_BuildAddColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} ADD COLUMN {SQL_PART}",
		BigQueryDialect{}.BuildAddColumnQuery(fakeTableID, "{SQL_PART}"),
	)
}

func TestBigQueryDialect_BuildIsNotToastValueExpression(t *testing.T) {
	assert.Equal(t,
		"TO_JSON_STRING(tbl.`bar`) NOT LIKE '%__debezium_unavailable_value%'",
		BigQueryDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.Invalid)),
	)
	assert.Equal(t,
		"TO_JSON_STRING(tbl.`foo`) NOT LIKE '%__debezium_unavailable_value%'",
		BigQueryDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Struct)),
	)
}

func TestBigQueryDialect_BuildMergeQueries_TempTable(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("order_id", typing.Integer),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("customers.orders")

	statements, err := BigQueryDialect{}.BuildMergeQueries(
		fakeTableID,
		"customers.orders_tmp",
		[]columns.Column{cols[0]},
		nil,
		cols,
		false,
		false,
	)
	assert.NoError(t, err)
	assert.Len(t, statements, 1)
	assert.Equal(t, []string{
		"MERGE INTO customers.orders tgt USING customers.orders_tmp AS stg ON tgt.`order_id` = stg.`order_id`",
		"WHEN MATCHED AND stg.`__artie_delete` THEN DELETE",
		"WHEN MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN UPDATE SET `order_id`=stg.`order_id`,`name`=stg.`name`",
		"WHEN NOT MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN INSERT (`order_id`,`name`) VALUES (stg.`order_id`,stg.`name`);",
	},
		strings.Split(strings.TrimSpace(statements[0]), "\n"))
}

func TestBigQueryDialect_BuildMergeQueries_SoftDelete(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("order_id", typing.Integer),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("customers.orders")

	statements, err := BigQueryDialect{}.BuildMergeQueries(
		fakeTableID,
		"{SUB_QUERY}",
		[]columns.Column{cols[0]},
		nil,
		cols,
		true,
		false,
	)
	assert.NoError(t, err)
	assert.Len(t, statements, 1)
	assert.Equal(t, []string{
		"MERGE INTO customers.orders tgt USING {SUB_QUERY} AS stg ON tgt.`order_id` = stg.`order_id`",
		"WHEN MATCHED AND IFNULL(stg.`__artie_only_set_delete`, false) = false THEN UPDATE SET `order_id`=stg.`order_id`,`name`=stg.`name`,`__artie_delete`=stg.`__artie_delete`",
		"WHEN MATCHED AND IFNULL(stg.`__artie_only_set_delete`, false) = true THEN UPDATE SET `__artie_delete`=stg.`__artie_delete`",
		"WHEN NOT MATCHED THEN INSERT (`order_id`,`name`,`__artie_delete`) VALUES (stg.`order_id`,stg.`name`,stg.`__artie_delete`);",
	},
		strings.Split(strings.TrimSpace(statements[0]), "\n"))
}

func TestBigQueryDialect_BuildMergeQueries_JSONKey(t *testing.T) {
	orderOIDCol := columns.NewColumn("order_oid", typing.Struct)
	var cols columns.Columns
	cols.AddColumn(orderOIDCol)
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))
	cols.AddColumn(columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("customers.orders")

	statements, err := BigQueryDialect{}.BuildMergeQueries(
		fakeTableID,
		"customers.orders_tmp",
		[]columns.Column{orderOIDCol},
		nil,
		cols.ValidColumns(),
		false,
		false,
	)
	assert.NoError(t, err)
	assert.Len(t, statements, 1)
	assert.Equal(t, []string{
		"MERGE INTO customers.orders tgt USING customers.orders_tmp AS stg ON TO_JSON_STRING(tgt.`order_oid`) = TO_JSON_STRING(stg.`order_oid`)",
		"WHEN MATCHED AND stg.`__artie_delete` THEN DELETE",
		"WHEN MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN UPDATE SET `order_oid`=stg.`order_oid`,`name`=stg.`name`",
		"WHEN NOT MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN INSERT (`order_oid`,`name`) VALUES (stg.`order_oid`,stg.`name`);",
	},
		strings.Split(strings.TrimSpace(statements[0]), "\n"))
}
