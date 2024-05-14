package dialect

import (
	"fmt"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
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
				OptionalStringPrecision: ptr.ToInt(12345),
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
		assert.Equal(t, 5, *kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, 2, kd.ExtendedDecimalDetails.Scale())
	}
	{
		kd, err := dialect.KindForDataType("bignumeric(5, 2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, 5, *kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, 2, kd.ExtendedDecimalDetails.Scale())
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
		"COALESCE(cc.`bar` != '__debezium_unavailable_value', true)",
		BigQueryDialect{}.BuildIsNotToastValueExpression(columns.NewColumn("bar", typing.Invalid), "cc"),
	)
	assert.Equal(t,
		"COALESCE(TO_JSON_STRING(cc.`foo`) != '{\"key\":\"__debezium_unavailable_value\"}', true)",
		BigQueryDialect{}.BuildIsNotToastValueExpression(columns.NewColumn("foo", typing.Struct), "cc"),
	)
}

func TestQuoteColumns(t *testing.T) {
	assert.Equal(t, []string{}, sql.QuoteColumns(nil, BigQueryDialect{}))
	cols := []columns.Column{columns.NewColumn("a", typing.Invalid), columns.NewColumn("b", typing.Invalid)}
	assert.Equal(t, []string{"`a`", "`b`"}, sql.QuoteColumns(cols, BigQueryDialect{}))
}

func TestBuildColumnsUpdateFragment(t *testing.T) {
	var lastCaseColTypes []columns.Column
	lastCaseCols := []string{"a1", "b2", "c3"}
	for _, lastCaseCol := range lastCaseCols {
		kd := typing.String
		var toast bool
		// a1 - struct + toast, b2 - string + toast, c3 = regular string.
		if lastCaseCol == "a1" {
			kd = typing.Struct
			toast = true
		} else if lastCaseCol == "b2" {
			toast = true
		}

		column := columns.NewColumn(lastCaseCol, kd)
		column.ToastColumn = toast
		lastCaseColTypes = append(lastCaseColTypes, column)
	}

	var lastCaseEscapeTypes []columns.Column
	lastCaseColsEsc := []string{"a1", "b2", "c3", "start", "select"}
	for _, lastCaseColEsc := range lastCaseColsEsc {
		kd := typing.String
		var toast bool
		// a1 - struct + toast, b2 - string + toast, c3 = regular string.
		if lastCaseColEsc == "a1" {
			kd = typing.Struct
			toast = true
		} else if lastCaseColEsc == "b2" {
			toast = true
		} else if lastCaseColEsc == "start" {
			kd = typing.Struct
			toast = true
		}

		column := columns.NewColumn(lastCaseColEsc, kd)
		column.ToastColumn = toast
		lastCaseEscapeTypes = append(lastCaseEscapeTypes, column)
	}

	lastCaseEscapeTypes = append(lastCaseEscapeTypes, columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	key := `{"key":"__debezium_unavailable_value"}`
	testCases := []struct {
		name           string
		columns        []columns.Column
		expectedString string
	}{
		{
			name:           "struct, string and toast string (bigquery)",
			columns:        lastCaseColTypes,
			expectedString: "`a1`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`a1`) != '{\"key\":\"__debezium_unavailable_value\"}', true) THEN cc.`a1` ELSE c.`a1` END,`b2`= CASE WHEN COALESCE(cc.`b2` != '__debezium_unavailable_value', true) THEN cc.`b2` ELSE c.`b2` END,`c3`=cc.`c3`",
		},
		{
			name:    "struct, string and toast string (bigquery) w/ reserved keywords",
			columns: lastCaseEscapeTypes,
			expectedString: fmt.Sprintf("`a1`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`a1`) != '%s', true) THEN cc.`a1` ELSE c.`a1` END,`b2`= CASE WHEN COALESCE(cc.`b2` != '__debezium_unavailable_value', true) THEN cc.`b2` ELSE c.`b2` END,`c3`=cc.`c3`,%s,%s",
				key, fmt.Sprintf("`start`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`start`) != '%s', true) THEN cc.`start` ELSE c.`start` END", key), "`select`=cc.`select`,`__artie_delete`=cc.`__artie_delete`"),
		},
	}

	for _, _testCase := range testCases {
		actualQuery := sql.BuildColumnsUpdateFragment(_testCase.columns, "cc", "c", BigQueryDialect{})
		assert.Equal(t, _testCase.expectedString, actualQuery, _testCase.name)
	}
}

func TestBigQueryDialect_BuildMergeQueries_TempTable(t *testing.T) {
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("order_id", typing.Integer))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("customers.orders")

	statements, err := BigQueryDialect{}.BuildMergeQueries(
		fakeTableID,
		"customers.orders_tmp",
		"",
		[]columns.Column{columns.NewColumn("order_id", typing.Invalid)},
		nil,
		cols.ValidColumns(),
		false,
		false,
	)
	assert.NoError(t, err)
	assert.Len(t, statements, 1)
	assert.Contains(t, statements[0], "MERGE INTO customers.orders c USING customers.orders_tmp AS cc ON c.`order_id` = cc.`order_id`")
}

func TestBigQueryDialect_BuildMergeQueries_JSONKey(t *testing.T) {
	orderOIDCol := columns.NewColumn("order_oid", typing.Struct)
	var cols columns.Columns
	cols.AddColumn(orderOIDCol)
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("customers.orders")

	statements, err := BigQueryDialect{}.BuildMergeQueries(
		fakeTableID,
		"customers.orders_tmp",
		"",
		[]columns.Column{orderOIDCol},
		nil,
		cols.ValidColumns(),
		false,
		false,
	)
	assert.Len(t, statements, 1)
	assert.NoError(t, err)
	assert.Contains(t, statements[0], "MERGE INTO customers.orders c USING customers.orders_tmp AS cc ON TO_JSON_STRING(c.`order_oid`) = TO_JSON_STRING(cc.`order_oid`)")
}
