package dialect

import (
	"fmt"
	"sort"
	"testing"

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

func buildColumns(colTypesMap map[string]typing.KindDetails) *columns.Columns {
	colNames := []string{}
	for colName := range colTypesMap {
		colNames = append(colNames, colName)
	}
	// Sort the column names alphabetically to ensure deterministic order
	sort.Strings(colNames)

	var cols columns.Columns
	for _, colName := range colNames {
		cols.AddColumn(columns.NewColumn(colName, colTypesMap[colName]))
	}

	return &cols
}

func TestSnowflakeDialect_BuildMergeQueries_SoftDelete(t *testing.T) {
	// No idempotent key
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                         typing.String,
		"bar":                        typing.String,
		"updated_at":                 typing.ETime,
		constants.DeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	{
		statements, err := SnowflakeDialect{}.BuildMergeQueries(
			fakeTableID,
			fqTable,
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
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED THEN UPDATE SET "__ARTIE_DELETE"=stg."__ARTIE_DELETE","BAR"=stg."BAR","ID"=stg."ID","UPDATED_AT"=stg."UPDATED_AT"
WHEN NOT MATCHED THEN INSERT ("__ARTIE_DELETE","BAR","ID","UPDATED_AT") VALUES (stg."__ARTIE_DELETE",stg."BAR",stg."ID",stg."UPDATED_AT");`, statements[0])
	}
	{
		statements, err := SnowflakeDialect{}.BuildMergeQueries(
			fakeTableID,
			fqTable,
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
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED AND stg.updated_at >= tgt.updated_at THEN UPDATE SET "__ARTIE_DELETE"=stg."__ARTIE_DELETE","BAR"=stg."BAR","ID"=stg."ID","UPDATED_AT"=stg."UPDATED_AT"
WHEN NOT MATCHED THEN INSERT ("__ARTIE_DELETE","BAR","ID","UPDATED_AT") VALUES (stg."__ARTIE_DELETE",stg."BAR",stg."ID",stg."UPDATED_AT");`, statements[0])
	}
}

func TestSnowflakeDialect_BuildMergeQueries(t *testing.T) {
	// No idempotent key
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                         typing.String,
		"bar":                        typing.String,
		"updated_at":                 typing.String,
		"start":                      typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
		fakeTableID,
		fqTable,
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
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN UPDATE SET "BAR"=stg."BAR","ID"=stg."ID","START"=stg."START","UPDATED_AT"=stg."UPDATED_AT"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("BAR","ID","START","UPDATED_AT") VALUES (stg."BAR",stg."ID",stg."START",stg."UPDATED_AT");`, statements[0])
}

func TestSnowflakeDialect_BuildMergeQueries_IdempotentKey(t *testing.T) {
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                         typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
		fakeTableID,
		fqTable,
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
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false AND stg.updated_at >= tgt.updated_at THEN UPDATE SET "ID"=stg."ID"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("ID") VALUES (stg."ID");`, statements[0])
}

func TestSnowflakeDialect_BuildMergeQueries_CompositeKey(t *testing.T) {
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                         typing.String,
		"another_id":                 typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
		fakeTableID,
		fqTable,
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
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID" AND tgt."ANOTHER_ID" = stg."ANOTHER_ID"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false AND stg.updated_at >= tgt.updated_at THEN UPDATE SET "ANOTHER_ID"=stg."ANOTHER_ID","ID"=stg."ID"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("ANOTHER_ID","ID") VALUES (stg."ANOTHER_ID",stg."ID");`, statements[0])
}

func TestSnowflakeDialect_BuildMergeQueries_EscapePrimaryKeys(t *testing.T) {
	// No idempotent key
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                         typing.String,
		"group":                      typing.String,
		"updated_at":                 typing.String,
		"start":                      typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
		fakeTableID,
		fqTable,
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
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID" AND tgt."GROUP" = stg."GROUP"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN UPDATE SET "GROUP"=stg."GROUP","ID"=stg."ID","START"=stg."START","UPDATED_AT"=stg."UPDATED_AT"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("GROUP","ID","START","UPDATED_AT") VALUES (stg."GROUP",stg."ID",stg."START",stg."UPDATED_AT");`, statements[0])
}
