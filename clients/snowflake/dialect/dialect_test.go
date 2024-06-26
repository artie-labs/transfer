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
		assert.Equal(t, int32(38), kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
	}
	{
		kd, err := SnowflakeDialect{}.KindForDataType("NUMBER(38, 2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, int32(38), kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
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

func TestSnowflakeDialect_BuildIsNotToastValueExpression(t *testing.T) {
	assert.Equal(t,
		`COALESCE(tbl."BAR" != '__debezium_unavailable_value', true)`,
		SnowflakeDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.Invalid)),
	)
	assert.Equal(t,
		`COALESCE(tbl."FOO" != {'key': '__debezium_unavailable_value'}, true)`,
		SnowflakeDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Struct)),
	)
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

	dateValue := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', false)", "1", "456", dateValue.Round(0).Format(time.RFC3339)),
		fmt.Sprintf("('%s', '%s', '%v', true)", "2", "bb", dateValue.Round(0).Format(time.RFC3339)), // Delete row 2.
		fmt.Sprintf("('%s', '%s', '%v', false)", "3", "dd", dateValue.Round(0).Format(time.RFC3339)),
	}

	// select stg.foo, stg.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	var _cols columns.Columns
	_cols.AddColumn(columns.NewColumn("id", typing.String))
	_cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	{
		statements, err := SnowflakeDialect{}.BuildMergeQueries(
			fakeTableID,
			subQuery,
			"",
			[]columns.Column{columns.NewColumn("id", typing.Invalid)},
			nil,
			_cols.ValidColumns(),
			true,
			false,
		)
		assert.Len(t, statements, 1)
		assert.NoError(t, err)
		assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( SELECT id,bar,updated_at,__artie_delete from (values ('1', '456', '2001-02-03T04:05:06Z', false),('2', 'bb', '2001-02-03T04:05:06Z', true),('3', 'dd', '2001-02-03T04:05:06Z', false)) as _tbl(id,bar,updated_at,__artie_delete) ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED THEN UPDATE SET "ID"=stg."ID","__ARTIE_DELETE"=stg."__ARTIE_DELETE"
WHEN NOT MATCHED THEN INSERT ("ID","__ARTIE_DELETE") VALUES (stg."ID",stg."__ARTIE_DELETE");`, statements[0])
	}
	{
		statements, err := SnowflakeDialect{}.BuildMergeQueries(
			fakeTableID,
			subQuery,
			"updated_at",
			[]columns.Column{columns.NewColumn("id", typing.Invalid)},
			nil,
			_cols.ValidColumns(),
			true,
			false,
		)
		assert.NoError(t, err)
		assert.Len(t, statements, 1)
		assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( SELECT id,bar,updated_at,__artie_delete from (values ('1', '456', '2001-02-03T04:05:06Z', false),('2', 'bb', '2001-02-03T04:05:06Z', true),('3', 'dd', '2001-02-03T04:05:06Z', false)) as _tbl(id,bar,updated_at,__artie_delete) ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED AND stg.updated_at >= tgt.updated_at THEN UPDATE SET "ID"=stg."ID","__ARTIE_DELETE"=stg."__ARTIE_DELETE"
WHEN NOT MATCHED THEN INSERT ("ID","__ARTIE_DELETE") VALUES (stg."ID",stg."__ARTIE_DELETE");`, statements[0])
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

	dateValue := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "1", "456", "foo", dateValue.Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "2", "bb", "bar", dateValue.Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "3", "dd", "world", dateValue.Round(0).UTC()),
	}

	// select stg.foo, stg.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
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
	assert.NoError(t, err)
	assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( SELECT id,bar,updated_at,start,__artie_delete from (values ('1', '456', 'foo', '2001-02-03 04:05:06 +0000 UTC', false),('2', 'bb', 'bar', '2001-02-03 04:05:06 +0000 UTC', false),('3', 'dd', 'world', '2001-02-03 04:05:06 +0000 UTC', false)) as _tbl(id,bar,updated_at,start,__artie_delete) ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN UPDATE SET "ID"=stg."ID","BAR"=stg."BAR","UPDATED_AT"=stg."UPDATED_AT","START"=stg."START"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("ID","BAR","UPDATED_AT","START") VALUES (stg."ID",stg."BAR",stg."UPDATED_AT",stg."START");`, statements[0])
}

func TestSnowflakeDialect_BuildMergeQueries_IdempotentKey(t *testing.T) {
	fqTable := "database.schema.table"
	cols := []string{
		"id",
		"bar",
		"updated_at",
		constants.DeleteColumnMarker,
	}

	dateValue := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', false)", "1", "456", dateValue.Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', false)", "2", "bb", dateValue.Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', false)", "3", "dd", dateValue.Round(0).UTC()),
	}

	// select stg.foo, stg.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
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
	assert.NoError(t, err)
	assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( SELECT id,bar,updated_at,__artie_delete from (values ('1', '456', '2001-02-03 04:05:06 +0000 UTC', false),('2', 'bb', '2001-02-03 04:05:06 +0000 UTC', false),('3', 'dd', '2001-02-03 04:05:06 +0000 UTC', false)) as _tbl(id,bar,updated_at,__artie_delete) ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false AND stg.updated_at >= tgt.updated_at THEN UPDATE SET "ID"=stg."ID"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("ID") VALUES (stg."ID");`, statements[0])
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

	dateValue := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%s', '%v', false)", "1", "3", "456", dateValue.Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%s', '%v', false)", "2", "2", "bb", dateValue.Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%s', '%v', false)", "3", "1", "dd", dateValue.Round(0).UTC()),
	}

	// select stg.foo, stg.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
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
	assert.NoError(t, err)
	assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( SELECT id,another_id,bar,updated_at,__artie_delete from (values ('1', '3', '456', '2001-02-03 04:05:06 +0000 UTC', false),('2', '2', 'bb', '2001-02-03 04:05:06 +0000 UTC', false),('3', '1', 'dd', '2001-02-03 04:05:06 +0000 UTC', false)) as _tbl(id,another_id,bar,updated_at,__artie_delete) ) AS stg ON tgt."ID" = stg."ID" AND tgt."ANOTHER_ID" = stg."ANOTHER_ID"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false AND stg.updated_at >= tgt.updated_at THEN UPDATE SET "ID"=stg."ID","ANOTHER_ID"=stg."ANOTHER_ID"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("ID","ANOTHER_ID") VALUES (stg."ID",stg."ANOTHER_ID");`, statements[0])
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

	dateValue := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "1", "456", "foo", dateValue.Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "2", "bb", "bar", dateValue.Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "3", "dd", "world", dateValue.Round(0).UTC()),
	}

	// select stg.foo, stg.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
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
	assert.NoError(t, err)
	assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( SELECT id,group,updated_at,start,__artie_delete from (values ('1', '456', 'foo', '2001-02-03 04:05:06 +0000 UTC', false),('2', 'bb', 'bar', '2001-02-03 04:05:06 +0000 UTC', false),('3', 'dd', 'world', '2001-02-03 04:05:06 +0000 UTC', false)) as _tbl(id,group,updated_at,start,__artie_delete) ) AS stg ON tgt."ID" = stg."ID" AND tgt."GROUP" = stg."GROUP"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN UPDATE SET "ID"=stg."ID","GROUP"=stg."GROUP","UPDATED_AT"=stg."UPDATED_AT","START"=stg."START"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("ID","GROUP","UPDATED_AT","START") VALUES (stg."ID",stg."GROUP",stg."UPDATED_AT",stg."START");`, statements[0])
}
