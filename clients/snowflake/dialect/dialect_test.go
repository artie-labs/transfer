package dialect

import (
	"fmt"
	"strings"
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

func TestSnowflakeDialect_QuoteIdentifier(t *testing.T) {
	dialect := SnowflakeDialect{}
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("FOO"))
}

func TestSnowflakeDialect_DataTypeForKind(t *testing.T) {
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
		assert.Equal(t, tc.expected, SnowflakeDialect{}.DataTypeForKind(tc.kd, true), idx)
		assert.Equal(t, tc.expected, SnowflakeDialect{}.DataTypeForKind(tc.kd, false), idx)
	}
}

func TestSnowflakeDialect_KindForDataType_Number(t *testing.T) {
	{
		expectedIntegers := []string{"number(38, 0)", "number(2, 0)", "number(3, 0)"}
		for _, expectedInteger := range expectedIntegers {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedInteger, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Integer, kd, expectedInteger)
		}
	}
	{
		expectedFloats := []string{"number(38, 1)", "number(2, 2)", "number(2, 30)"}
		for _, expectedFloat := range expectedFloats {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedFloat, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind, expectedFloat)
		}
	}
}

func TestSnowflakeDialect_KindForDataType_Floats(t *testing.T) {
	{
		expectedFloats := []string{"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE",
			"DOUBLE PRECISION", "REAL"}
		for _, expectedFloat := range expectedFloats {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedFloat, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Float, kd, expectedFloat)
		}
	}
	{
		// Invalid because precision nor scale is included.
		kd, err := SnowflakeDialect{}.KindForDataType("NUMERIC", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Invalid, kd)
	}
	{
		kd, err := SnowflakeDialect{}.KindForDataType("NUMERIC(38, 2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, 38, *kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, 2, kd.ExtendedDecimalDetails.Scale())
	}
	{
		kd, err := SnowflakeDialect{}.KindForDataType("NUMBER(38, 2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, 38, *kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, 2, kd.ExtendedDecimalDetails.Scale())
	}
	{
		kd, err := SnowflakeDialect{}.KindForDataType("DECIMAL", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
	}
}

func TestSnowflakeDialect_KindForDataType_Integer(t *testing.T) {
	expectedIntegers := []string{"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"}
	for _, expectedInteger := range expectedIntegers {
		kd, err := SnowflakeDialect{}.KindForDataType(expectedInteger, "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Integer, kd, expectedInteger)
	}
}

func TestSnowflakeDialect_KindForDataType_Other(t *testing.T) {
	expectedStrings := []string{"CHARACTER", "CHAR", "STRING", "TEXT"}
	for _, expectedString := range expectedStrings {
		kd, err := SnowflakeDialect{}.KindForDataType(expectedString, "")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kd, expectedString)
	}

	{
		kd, err := SnowflakeDialect{}.KindForDataType("VARCHAR (255)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.String.Kind, kd.Kind)
		assert.Equal(t, 255, *kd.OptionalStringPrecision)
	}
}

func TestSnowflakeDialect_KindForDataType_DateTime(t *testing.T) {
	expectedDateTimes := []string{"DATETIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ(9)", "TIMESTAMP_TZ"}
	for _, expectedDateTime := range expectedDateTimes {
		kd, err := SnowflakeDialect{}.KindForDataType(expectedDateTime, "")
		assert.NoError(t, err)
		assert.Equal(t, ext.DateTime.Type, kd.ExtendedTimeDetails.Type, expectedDateTime)
	}
}

func TestSnowflakeDialect_KindForDataType_Complex(t *testing.T) {
	{
		expectedStructs := []string{"variant", "VaRIANT", "OBJECT"}
		for _, expectedStruct := range expectedStructs {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedStruct, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Struct, kd, expectedStruct)
		}
	}
	{
		kd, err := SnowflakeDialect{}.KindForDataType("boolean", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		kd, err := SnowflakeDialect{}.KindForDataType("ARRAY", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kd)
	}
}

func TestSnowflakeDialect_KindForDataType_Errors(t *testing.T) {
	{
		kd, err := SnowflakeDialect{}.KindForDataType("", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Invalid, kd)
	}
	{
		kd, err := SnowflakeDialect{}.KindForDataType("abc123", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Invalid, kd)
	}
}

func TestSnowflakeDialect_KindForDataType_NoDataLoss(t *testing.T) {
	kindDetails := []typing.KindDetails{
		typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		typing.String,
		typing.Boolean,
		typing.Struct,
	}

	for _, kindDetail := range kindDetails {
		kd, err := SnowflakeDialect{}.KindForDataType(SnowflakeDialect{}.DataTypeForKind(kindDetail, false), "")
		assert.NoError(t, err)
		assert.Equal(t, kindDetail, kd)
	}
}

func TestSnowflakeDialect_IsColumnAlreadyExistsErr(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		expectedResult bool
	}{
		{
			name:           "Snowflake, column already exists error",
			err:            fmt.Errorf("Column already exists"),
			expectedResult: true,
		},
		{
			name: "Snowflake, random error",
			err:  fmt.Errorf("hello there qux"),
		},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expectedResult, SnowflakeDialect{}.IsColumnAlreadyExistsErr(tc.err), tc.name)
	}
}

func TestSnowflakeDialect_IsTableDoesNotExistErr(t *testing.T) {
	errToExpectation := map[error]bool{
		nil: false,
		fmt.Errorf("Table 'DATABASE.SCHEMA.TABLE' does not exist or not authorized"): true,
		fmt.Errorf("hi this is super random"):                                        false,
	}

	for err, expectation := range errToExpectation {
		assert.Equal(t, SnowflakeDialect{}.IsTableDoesNotExistErr(err), expectation, err)
	}
}

func TestSnowflakeDialect_BuildCreateTableQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	// Temporary:
	assert.Equal(t,
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2}) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE)`,
		SnowflakeDialect{}.BuildCreateTableQuery(fakeTableID, true, []string{"{PART_1}", "{PART_2}"}),
	)
	// Not temporary:
	assert.Equal(t,
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2})`,
		SnowflakeDialect{}.BuildCreateTableQuery(fakeTableID, false, []string{"{PART_1}", "{PART_2}"}),
	)
}

func TestSnowflakeDialect_BuildAlterColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} drop COLUMN {SQL_PART}",
		SnowflakeDialect{}.BuildAlterColumnQuery(fakeTableID, constants.Delete, "{SQL_PART}"),
	)
}

func TestSnowflakeDialect_BuildIsToastColExpression(t *testing.T) {
	assert.Equal(t,
		`COALESCE(cc."BAR" != '__debezium_unavailable_value', true)`,
		SnowflakeDialect{}.BuildIsToastColExpression(columns.NewColumn("bar", typing.Invalid)),
	)
	assert.Equal(t,
		`COALESCE(cc."FOO" != {'key': '__debezium_unavailable_value'}, true)`,
		SnowflakeDialect{}.BuildIsToastColExpression(columns.NewColumn("foo", typing.Struct)),
	)
}

func TestQuoteColumns(t *testing.T) {
	assert.Equal(t, []string{}, sql.QuoteColumns(nil, SnowflakeDialect{}))
	cols := []columns.Column{columns.NewColumn("a", typing.Invalid), columns.NewColumn("b", typing.Invalid)}
	assert.Equal(t, []string{`"A"`, `"B"`}, sql.QuoteColumns(cols, SnowflakeDialect{}))
}

func TestBuildColumnsUpdateFragment(t *testing.T) {
	var stringAndToastCols []columns.Column
	for _, col := range []string{"foo", "bar"} {
		var toastCol bool
		if col == "foo" {
			toastCol = true
		}

		column := columns.NewColumn(col, typing.String)
		column.ToastColumn = toastCol
		stringAndToastCols = append(stringAndToastCols, column)
	}

	actualQuery := sql.BuildColumnsUpdateFragment(stringAndToastCols, SnowflakeDialect{})
	assert.Equal(t, `"FOO"= CASE WHEN COALESCE(cc."FOO" != '__debezium_unavailable_value', true) THEN cc."FOO" ELSE c."FOO" END,"BAR"=cc."BAR"`, actualQuery)
}

func TestSnowflakeDialect_BuildMergeQueries_SoftDelete(t *testing.T) {
	// No idempotent key
	fqTable := "database.schema.table"
	cols := []string{
		"id",
		"bar",
		"updated_at",
		constants.DeleteColumnMarker,
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', false)", "1", "456", time.Now().Round(0).Format(time.RFC3339)),
		fmt.Sprintf("('%s', '%s', '%v', true)", "2", "bb", time.Now().Round(0).Format(time.RFC3339)), // Delete row 2.
		fmt.Sprintf("('%s', '%s', '%v', false)", "3", "dd", time.Now().Round(0).Format(time.RFC3339)),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	var _cols columns.Columns
	_cols.AddColumn(columns.NewColumn("id", typing.String))
	_cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)
	for _, idempotentKey := range []string{"", "updated_at"} {
		statements, err := SnowflakeDialect{}.BuildMergeQueries(
			fakeTableID,
			subQuery,
			idempotentKey,
			[]columns.Column{columns.NewColumn("id", typing.Invalid)},
			nil,
			_cols.ValidColumns(),
			true,
			false,
		)
		assert.Len(t, statements, 1)
		mergeSQL := statements[0]
		assert.NoError(t, err)
		assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
		// Soft deletion flag being passed.
		assert.Contains(t, mergeSQL, `"__ARTIE_DELETE"=cc."__ARTIE_DELETE"`, mergeSQL)

		assert.Equal(t, len(idempotentKey) > 0, strings.Contains(mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at")))
	}
}

func TestSnowflakeDialect_BuildMergeQueries(t *testing.T) {
	// No idempotent key
	fqTable := "database.schema.table"
	colToTypes := map[string]typing.KindDetails{
		"id":                         typing.String,
		"bar":                        typing.String,
		"updated_at":                 typing.String,
		"start":                      typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	}

	// This feels a bit round about, but this is because iterating over a map is not deterministic.
	cols := []string{"id", "bar", "updated_at", "start", constants.DeleteColumnMarker}
	var _cols columns.Columns
	for _, col := range cols {
		_cols.AddColumn(columns.NewColumn(col, colToTypes[col]))
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "1", "456", "foo", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "2", "bb", "bar", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "3", "dd", "world", time.Now().Round(0).UTC()),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
		fakeTableID,
		subQuery,
		"",
		[]columns.Column{columns.NewColumn("id", typing.Invalid)},
		nil,
		_cols.ValidColumns(),
		false,
		false,
	)
	assert.Len(t, statements, 1)
	mergeSQL := statements[0]
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.NotContains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", `"UPDATED_AT"`, `"UPDATED_AT"`), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	// Check primary keys clause
	assert.Contains(t, mergeSQL, `AS cc ON c."ID" = cc."ID"`, mergeSQL)

	// Check setting for update
	assert.Contains(t, mergeSQL, `SET "ID"=cc."ID","BAR"=cc."BAR","UPDATED_AT"=cc."UPDATED_AT","START"=cc."START"`, mergeSQL)
	// Check for INSERT
	assert.Contains(t, mergeSQL, `"ID","BAR","UPDATED_AT","START"`, mergeSQL)
	assert.Contains(t, mergeSQL, `cc."ID",cc."BAR",cc."UPDATED_AT",cc."START"`, mergeSQL)
}

func TestSnowflakeDialect_BuildMergeQueries_IdempotentKey(t *testing.T) {
	fqTable := "database.schema.table"
	cols := []string{
		"id",
		"bar",
		"updated_at",
		constants.DeleteColumnMarker,
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', false)", "1", "456", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', false)", "2", "bb", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', false)", "3", "dd", time.Now().Round(0).UTC()),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	var _cols columns.Columns
	_cols.AddColumn(columns.NewColumn("id", typing.String))
	_cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
		fakeTableID,
		subQuery,
		"updated_at",
		[]columns.Column{columns.NewColumn("id", typing.Invalid)},
		nil,
		_cols.ValidColumns(),
		false,
		false,
	)
	assert.Len(t, statements, 1)
	mergeSQL := statements[0]
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.Contains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at"), fmt.Sprintf("Idempotency key: %s", mergeSQL))
}

func TestSnowflakeDialect_BuildMergeQueries_CompositeKey(t *testing.T) {
	fqTable := "database.schema.table"
	cols := []string{
		"id",
		"another_id",
		"bar",
		"updated_at",
		constants.DeleteColumnMarker,
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%s', '%v', false)", "1", "3", "456", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%s', '%v', false)", "2", "2", "bb", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%s', '%v', false)", "3", "1", "dd", time.Now().Round(0).UTC()),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	var _cols columns.Columns
	_cols.AddColumn(columns.NewColumn("id", typing.String))
	_cols.AddColumn(columns.NewColumn("another_id", typing.String))
	_cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
		fakeTableID,
		subQuery,
		"updated_at",
		[]columns.Column{
			columns.NewColumn("id", typing.Invalid),
			columns.NewColumn("another_id", typing.Invalid),
		},
		nil,
		_cols.ValidColumns(),
		false,
		false,
	)
	assert.Len(t, statements, 1)
	mergeSQL := statements[0]
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.Contains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at"), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	assert.Contains(t, mergeSQL, `cc ON c."ID" = cc."ID" and c."ANOTHER_ID" = cc."ANOTHER_ID"`, mergeSQL)
}

func TestSnowflakeDialect_BuildMergeQueries_EscapePrimaryKeys(t *testing.T) {
	// No idempotent key
	fqTable := "database.schema.table"
	colToTypes := map[string]typing.KindDetails{
		"id":                         typing.String,
		"group":                      typing.String,
		"updated_at":                 typing.String,
		"start":                      typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	}

	// This feels a bit round about, but this is because iterating over a map is not deterministic.
	cols := []string{"id", "group", "updated_at", "start", constants.DeleteColumnMarker}
	var _cols columns.Columns
	for _, col := range cols {
		_cols.AddColumn(columns.NewColumn(col, colToTypes[col]))
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "1", "456", "foo", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "2", "bb", "bar", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "3", "dd", "world", time.Now().Round(0).UTC()),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
		fakeTableID,
		subQuery,
		"",
		[]columns.Column{
			columns.NewColumn("id", typing.Invalid),
			columns.NewColumn("group", typing.Invalid),
		},
		nil,
		_cols.ValidColumns(),
		false,
		false,
	)
	assert.Len(t, statements, 1)
	mergeSQL := statements[0]
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.NotContains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", `"UPDATED_AT"`, `"UPDATED_AT"`), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	// Check primary keys clause
	assert.Contains(t, mergeSQL, `AS cc ON c."ID" = cc."ID" and c."GROUP" = cc."GROUP"`, mergeSQL)
	// Check setting for update
	assert.Contains(t, mergeSQL, `SET "ID"=cc."ID","GROUP"=cc."GROUP","UPDATED_AT"=cc."UPDATED_AT","START"=cc."START"`, mergeSQL)
	// Check for INSERT
	assert.Contains(t, mergeSQL, `"ID","GROUP","UPDATED_AT","START"`, mergeSQL)
	assert.Contains(t, mergeSQL, `cc."ID",cc."GROUP",cc."UPDATED_AT",cc."START"`, mergeSQL)
}
