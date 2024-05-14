package dialect

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestRedshiftDialect_QuoteIdentifier(t *testing.T) {
	dialect := RedshiftDialect{}
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("FOO"))
}

func TestRedshiftDialect_DataTypeForKind(t *testing.T) {
	tcs := []struct {
		kd       typing.KindDetails
		expected string
	}{
		{
			kd:       typing.String,
			expected: "VARCHAR(MAX)",
		},
		{
			kd: typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: ptr.ToInt(12345),
			},
			expected: "VARCHAR(12345)",
		},
	}

	for idx, tc := range tcs {
		assert.Equal(t, tc.expected, RedshiftDialect{}.DataTypeForKind(tc.kd, true), idx)
		assert.Equal(t, tc.expected, RedshiftDialect{}.DataTypeForKind(tc.kd, false), idx)
	}
}

func TestRedshiftDialect_KindForDataType(t *testing.T) {
	dialect := RedshiftDialect{}

	type rawTypeAndPrecision struct {
		rawType   string
		precision string
	}

	type _testCase struct {
		name       string
		rawTypes   []rawTypeAndPrecision
		expectedKd typing.KindDetails
	}

	testCases := []_testCase{
		{
			name: "Integer",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "integer"},
				{rawType: "bigint"},
				{rawType: "INTEGER"},
			},
			expectedKd: typing.Integer,
		},
		{
			name: "String w/o precision",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "character varying"},
				{rawType: "character varying(65535)"},
				{
					rawType:   "character varying",
					precision: "not a number",
				},
			},
			expectedKd: typing.String,
		},
		{
			name: "String w/ precision",
			rawTypes: []rawTypeAndPrecision{
				{
					rawType:   "character varying",
					precision: "65535",
				},
			},
			expectedKd: typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: ptr.ToInt(65535),
			},
		},
		{
			name: "Double Precision",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "double precision"},
				{rawType: "DOUBLE precision"},
			},
			expectedKd: typing.Float,
		},
		{
			name: "Time",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "timestamp with time zone"},
				{rawType: "timestamp without time zone"},
				{rawType: "time without time zone"},
				{rawType: "date"},
			},
			expectedKd: typing.ETime,
		},
		{
			name: "Boolean",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "boolean"},
			},
			expectedKd: typing.Boolean,
		},
		{
			name: "numeric",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "numeric(5,2)"},
				{rawType: "numeric(5,5)"},
			},
			expectedKd: typing.EDecimal,
		},
	}

	for _, testCase := range testCases {
		for _, rawTypeAndPrec := range testCase.rawTypes {
			kd, err := dialect.KindForDataType(rawTypeAndPrec.rawType, rawTypeAndPrec.precision)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedKd.Kind, kd.Kind, testCase.name)

			if kd.OptionalStringPrecision != nil {
				assert.Equal(t, *testCase.expectedKd.OptionalStringPrecision, *kd.OptionalStringPrecision, testCase.name)
			} else {
				assert.Nil(t, kd.OptionalStringPrecision, testCase.name)
			}
		}
	}

	{
		kd, err := dialect.KindForDataType("numeric(5,2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, 5, *kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, 2, kd.ExtendedDecimalDetails.Scale())
	}
}

func TestRedshiftDialect_IsColumnAlreadyExistsErr(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		expectedResult bool
	}{
		{
			name:           "Redshift actual error",
			err:            fmt.Errorf(`ERROR: column "foo" of relation "statement" already exists [ErrorId: 1-64da9ea9]`),
			expectedResult: true,
		},
		{
			name: "Redshift error, but irrelevant",
			err:  fmt.Errorf("foo"),
		},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expectedResult, RedshiftDialect{}.IsColumnAlreadyExistsErr(tc.err), tc.name)
	}
}

func TestRedshiftDialect_BuildCreateTableQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	// Temporary:
	assert.Equal(t,
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2});`,
		RedshiftDialect{}.BuildCreateTableQuery(fakeTableID, true, []string{"{PART_1}", "{PART_2}"}),
	)
	// Not temporary:
	assert.Equal(t,
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2});`,
		RedshiftDialect{}.BuildCreateTableQuery(fakeTableID, false, []string{"{PART_1}", "{PART_2}"}),
	)
}

func TestRedshiftDialect_BuildAlterColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} drop COLUMN {SQL_PART}",
		RedshiftDialect{}.BuildAlterColumnQuery(fakeTableID, constants.Delete, "{SQL_PART}"),
	)
}

func TestQuoteIdentifiers(t *testing.T) {
	assert.Equal(t, []string{}, sql.QuoteIdentifiers([]string{}, RedshiftDialect{}))
	assert.Equal(t, []string{`"a"`, `"b"`, `"c"`}, sql.QuoteIdentifiers([]string{"a", "b", "c"}, RedshiftDialect{}))
}

func TestRedshiftDialect_BuildProcessToastColExpression(t *testing.T) {
	assert.Equal(t, `CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, RedshiftDialect{}.BuildProcessToastColExpression("bar"))
}

func TestRedshiftDialect_BuildProcessToastStructColExpression(t *testing.T) {
	assert.Equal(t, `CASE WHEN COALESCE(cc.foo != JSON_PARSE('{"key":"__debezium_unavailable_value"}'), true) THEN cc.foo ELSE c.foo END`, RedshiftDialect{}.BuildProcessToastStructColExpression("foo"))
}

func TestBuildColumnsUpdateFragment(t *testing.T) {
	var happyPathCols []columns.Column
	for _, col := range []string{"foo", "bar"} {
		column := columns.NewColumn(col, typing.String)
		column.ToastColumn = false
		happyPathCols = append(happyPathCols, column)
	}

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

	testCases := []struct {
		name           string
		columns        []columns.Column
		expectedString string
	}{
		{
			name:           "happy path",
			columns:        happyPathCols,
			expectedString: `"foo"=cc."foo","bar"=cc."bar"`,
		},
		{
			name:           "struct, string and toast string",
			columns:        lastCaseColTypes,
			expectedString: `"a1"= CASE WHEN COALESCE(cc."a1" != JSON_PARSE('{"key":"__debezium_unavailable_value"}'), true) THEN cc."a1" ELSE c."a1" END,"b2"= CASE WHEN COALESCE(cc."b2" != '__debezium_unavailable_value', true) THEN cc."b2" ELSE c."b2" END,"c3"=cc."c3"`,
		},
	}

	for _, _testCase := range testCases {
		actualQuery := sql.BuildColumnsUpdateFragment(_testCase.columns, RedshiftDialect{})
		assert.Equal(t, _testCase.expectedString, actualQuery, _testCase.name)
	}
}

func TestRedshiftDialect_EqualitySQLParts(t *testing.T) {
	assert.Equal(t,
		[]string{`c."col1" = cc."col1"`, `c."col2" = cc."col2"`},
		RedshiftDialect{}.equalitySQLParts([]columns.Column{columns.NewColumn("col1", typing.Invalid), columns.NewColumn("col2", typing.Invalid)}),
	)
}

func TestRedshiftDialect_BuildMergeInsertQuery(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
		columns.NewColumn("col3", typing.Invalid),
	}

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE_ID}")
	assert.Equal(t,
		`INSERT INTO {TABLE_ID} ("col1","col2","col3") SELECT cc."col1",cc."col2",cc."col3" FROM {SUB_QUERY} AS cc LEFT JOIN {TABLE_ID} AS c ON c."col1" = cc."col1" AND c."col3" = cc."col3" WHERE c."col1" IS NULL;`,
		RedshiftDialect{}.buildMergeInsertQuery(fakeTableID, "{SUB_QUERY}", []columns.Column{cols[0], cols[2]}, cols),
	)
}

func TestRedshiftDialect_BuildMergeUpdateQuery(t *testing.T) {
	testCases := []struct {
		name          string
		softDelete    bool
		idempotentKey string
		expected      string
	}{
		{
			name:       "soft delete enabled",
			softDelete: true,
			expected:   `UPDATE {TABLE_ID} AS c SET "col1"=cc."col1","col2"=cc."col2","col3"=cc."col3" FROM {SUB_QUERY} AS cc WHERE c."col1" = cc."col1" AND c."col3" = cc."col3";`,
		},
		{
			name:          "soft delete enabled + idempotent key",
			softDelete:    true,
			idempotentKey: "{ID_KEY}",
			expected:      `UPDATE {TABLE_ID} AS c SET "col1"=cc."col1","col2"=cc."col2","col3"=cc."col3" FROM {SUB_QUERY} AS cc WHERE c."col1" = cc."col1" AND c."col3" = cc."col3" AND cc.{ID_KEY} >= c.{ID_KEY};`,
		},
		{
			name:       "soft delete disabled",
			softDelete: false,
			expected:   `UPDATE {TABLE_ID} AS c SET "col1"=cc."col1","col2"=cc."col2","col3"=cc."col3" FROM {SUB_QUERY} AS cc WHERE c."col1" = cc."col1" AND c."col3" = cc."col3" AND COALESCE(cc."__artie_delete", false) = false;`,
		},
		{
			name:          "soft delete disabled + idempotent key",
			softDelete:    false,
			idempotentKey: "{ID_KEY}",
			expected:      `UPDATE {TABLE_ID} AS c SET "col1"=cc."col1","col2"=cc."col2","col3"=cc."col3" FROM {SUB_QUERY} AS cc WHERE c."col1" = cc."col1" AND c."col3" = cc."col3" AND cc.{ID_KEY} >= c.{ID_KEY} AND COALESCE(cc."__artie_delete", false) = false;`,
		},
	}

	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
		columns.NewColumn("col3", typing.Invalid),
	}

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE_ID}")

	for _, testCase := range testCases {
		actual := RedshiftDialect{}.buildMergeUpdateQuery(
			fakeTableID,
			"{SUB_QUERY}",
			[]columns.Column{cols[0], cols[2]},
			cols,
			testCase.idempotentKey,
			testCase.softDelete,
		)
		assert.Equal(t, testCase.expected, actual, testCase.name)
	}
}

func TestRedshiftDialect_BuildMergeDeleteQuery(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
		columns.NewColumn("col3", typing.Invalid),
	}

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE_ID}")
	assert.Equal(t,
		`DELETE FROM {TABLE_ID} WHERE ("col1","col2") IN (SELECT cc."col1",cc."col2" FROM {SUB_QUERY} AS cc WHERE cc."__artie_delete" = true);`,
		RedshiftDialect{}.buildMergeDeleteQuery(
			fakeTableID,
			"{SUB_QUERY}",
			[]columns.Column{cols[0], cols[1]},
		),
	)
}
