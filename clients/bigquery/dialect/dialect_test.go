package dialect

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/ptr"
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
	tcs := []struct {
		kd       typing.KindDetails
		expected string
	}{
		{
			kd:       typing.String,
			expected: "string",
		},
		{
			kd: typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: ptr.ToInt32(12345),
			},
			expected: "string",
		},
	}

	for idx, tc := range tcs {
		assert.Equal(t, tc.expected, BigQueryDialect{}.DataTypeForKind(tc.kd, true), idx)
		assert.Equal(t, tc.expected, BigQueryDialect{}.DataTypeForKind(tc.kd, false), idx)
	}
}

func TestBigQueryDialect_KindForDataType(t *testing.T) {
	dialect := BigQueryDialect{}

	bqColToExpectedKind := map[string]typing.KindDetails{
		// Number
		"numeric":           typing.EDecimal,
		"numeric(5)":        typing.Integer,
		"numeric(5, 0)":     typing.Integer,
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
		"datetime":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"timestamp": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"time":      typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		"date":      typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		//Invalid
		"foo":            typing.Invalid,
		"foofoo":         typing.Invalid,
		"":               typing.Invalid,
		"numeric(1,2,3)": typing.Invalid,
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
		typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
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
	testCases := []struct {
		name           string
		err            error
		expectedResult bool
	}{
		{
			name:           "BigQuery, column already exists error",
			err:            fmt.Errorf("Column already exists"),
			expectedResult: true,
		},
		{
			name: "BigQuery, random error",
			err:  fmt.Errorf("hello there qux"),
		},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expectedResult, BigQueryDialect{}.IsColumnAlreadyExistsErr(tc.err), tc.name)
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
		tsString := BQExpiresDate(birthday)
		ts, err := fromExpiresDateStringToTime(tsString)
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
