package dialect

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestBigQueryDialect_QuoteIdentifier(t *testing.T) {
	dialect := BigQueryDialect{}
	assert.Equal(t, "`foo`", dialect.QuoteIdentifier("foo"))
	assert.Equal(t, "`FOO`", dialect.QuoteIdentifier("FOO"))
}

func TestBigQueryDialect_DataTypeForKind(t *testing.T) {
	{
		// String
		{
			assert.Equal(t, "string", BigQueryDialect{}.DataTypeForKind(typing.String, false))
		}
		{
			assert.Equal(t, "string", BigQueryDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(12345))}, true))
		}
	}
}

func TestBigQueryDialect_KindForDataType(t *testing.T) {
	dialect := BigQueryDialect{}
	{
		// Numeric
		{
			// Invalid
			kd, err := dialect.KindForDataType("numeric(1,2,3)", "")
			assert.ErrorContains(t, err, "invalid number of parts: 3")
			assert.Equal(t, typing.Invalid, kd)
		}
		{
			// Numeric(5)
			kd, err := dialect.KindForDataType("NUMERIC(5)", "")
			assert.NoError(t, err)

			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), kd.ExtendedDecimalDetails.Scale())
			assert.Equal(t, "NUMERIC(5, 0)", kd.ExtendedDecimalDetails.BigQueryKind())

		}
		{
			// Numeric(5, 0)
			kd, err := dialect.KindForDataType("NUMERIC(5, 0)", "")
			assert.NoError(t, err)

			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), kd.ExtendedDecimalDetails.Scale())
			assert.Equal(t, "NUMERIC(5, 0)", kd.ExtendedDecimalDetails.BigQueryKind())
		}
	}

	bqColToExpectedKind := map[string]typing.KindDetails{
		// Number
		"numeric":           typing.EDecimal,
		"numeric(5, 2)":     typing.EDecimal,
		"numeric(8, 6)":     typing.EDecimal,
		"bignumeric(38, 2)": typing.EDecimal,

		// Integer
		"int":     typing.Integer,
		"integer": typing.Integer,
		"inT64":   typing.Integer,
		// String
		"varchar":     typing.String,
		"string":      typing.String,
		"sTriNG":      typing.String,
		"STRING (10)": typing.String,
		// Array
		"array<integer>": typing.Array,
		"array<string>":  typing.Array,
		// Boolean
		"bool":    typing.Boolean,
		"boolean": typing.Boolean,
		// Struct
		"STRUCT<foo STRING>": typing.Struct,
		"record":             typing.Struct,
		"json":               typing.Struct,
		// Datetime
		"datetime":  typing.MustNewExtendedTimeDetails(typing.ETime, ext.TimestampTZKindType, ""),
		"timestamp": typing.MustNewExtendedTimeDetails(typing.ETime, ext.TimestampTZKindType, ""),
		"time":      typing.MustNewExtendedTimeDetails(typing.ETime, ext.TimeKindType, ""),
		"date":      typing.MustNewExtendedTimeDetails(typing.ETime, ext.DateKindType, ""),
		//Invalid
		"foo":    typing.Invalid,
		"foofoo": typing.Invalid,
		"":       typing.Invalid,
	}

	for bqCol, expectedKind := range bqColToExpectedKind {
		kd, err := dialect.KindForDataType(bqCol, "")
		assert.NoError(t, err)
		assert.Equal(t, expectedKind.Kind, kd.Kind, bqCol)
	}

	{
		_, err := dialect.KindForDataType("numeric(5", "")
		assert.ErrorContains(t, err, "missing closing parenthesis")
	}
	{
		kd, err := dialect.KindForDataType("numeric(5, 2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
	}
	{
		kd, err := dialect.KindForDataType("bignumeric(5, 2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
	}
}

func TestBigQueryDialect_KindForDataType_NoDataLoss(t *testing.T) {
	kindDetails := []typing.KindDetails{
		typing.MustNewExtendedTimeDetails(typing.ETime, ext.TimestampTZKindType, ""),
		typing.MustNewExtendedTimeDetails(typing.ETime, ext.TimeKindType, ""),
		typing.MustNewExtendedTimeDetails(typing.ETime, ext.DateKindType, ""),
		typing.String,
		typing.Boolean,
		typing.Struct,
	}

	for _, kindDetail := range kindDetails {
		kd, err := BigQueryDialect{}.KindForDataType(BigQueryDialect{}.DataTypeForKind(kindDetail, false), "")
		assert.NoError(t, err)
		assert.Equal(t, kindDetail, kd)
	}
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

func TestBigQueryDialect_BuildAlterColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} drop COLUMN {SQL_PART}",
		BigQueryDialect{}.BuildAlterColumnQuery(fakeTableID, constants.Delete, "{SQL_PART}"),
	)
}

func TestBigQueryDialect_BuildIsNotToastValueExpression(t *testing.T) {
	assert.Equal(t,
		"COALESCE(tbl.`bar` != '__debezium_unavailable_value', true)",
		BigQueryDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.Invalid)),
	)
	assert.Equal(t,
		"COALESCE(TO_JSON_STRING(tbl.`foo`) != '{\"key\":\"__debezium_unavailable_value\"}', true)",
		BigQueryDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Struct)),
	)
}

func TestBigQueryDialect_BuildMergeQueries_TempTable(t *testing.T) {
	var cols = []columns.Column{
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
		"WHEN NOT MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN INSERT (`order_id`,`name`) VALUES (stg.`order_id`,stg.`name`);"},
		strings.Split(strings.TrimSpace(statements[0]), "\n"))
}

func TestBigQueryDialect_BuildMergeQueries_SoftDelete(t *testing.T) {
	var cols = []columns.Column{
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
		"WHEN NOT MATCHED THEN INSERT (`order_id`,`name`,`__artie_delete`) VALUES (stg.`order_id`,stg.`name`,stg.`__artie_delete`);"},
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
		"WHEN NOT MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN INSERT (`order_oid`,`name`) VALUES (stg.`order_oid`,stg.`name`);"},
		strings.Split(strings.TrimSpace(statements[0]), "\n"))
}
