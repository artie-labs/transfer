package dialect

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/ptr"
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

func TestRedshiftDialect_BuildIsNotToastValueExpression(t *testing.T) {
	assert.Equal(t,
		`COALESCE(tbl."bar" != '__debezium_unavailable_value', true)`,
		RedshiftDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.Invalid)),
	)
	assert.Equal(t,
		`COALESCE(tbl."foo" != JSON_PARSE('{"key":"__debezium_unavailable_value"}'), true)`,
		RedshiftDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Struct)),
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
		`INSERT INTO {TABLE_ID} ("col1","col2","col3") SELECT stg."col1",stg."col2",stg."col3" FROM {SUB_QUERY} AS stg LEFT JOIN {TABLE_ID} AS tgt ON tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" WHERE tgt."col1" IS NULL;`,
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
			expected:   `UPDATE {TABLE_ID} AS tgt SET "col1"=stg."col1","col2"=stg."col2","col3"=stg."col3" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3";`,
		},
		{
			name:          "soft delete enabled + idempotent key",
			softDelete:    true,
			idempotentKey: "{ID_KEY}",
			expected:      `UPDATE {TABLE_ID} AS tgt SET "col1"=stg."col1","col2"=stg."col2","col3"=stg."col3" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" AND stg.{ID_KEY} >= tgt.{ID_KEY};`,
		},
		{
			name:       "soft delete disabled",
			softDelete: false,
			expected:   `UPDATE {TABLE_ID} AS tgt SET "col1"=stg."col1","col2"=stg."col2","col3"=stg."col3" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" AND COALESCE(stg."__artie_delete", false) = false;`,
		},
		{
			name:          "soft delete disabled + idempotent key",
			softDelete:    false,
			idempotentKey: "{ID_KEY}",
			expected:      `UPDATE {TABLE_ID} AS tgt SET "col1"=stg."col1","col2"=stg."col2","col3"=stg."col3" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" AND stg.{ID_KEY} >= tgt.{ID_KEY} AND COALESCE(stg."__artie_delete", false) = false;`,
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
		`DELETE FROM {TABLE_ID} WHERE ("col1","col2") IN (SELECT stg."col1",stg."col2" FROM {SUB_QUERY} AS stg WHERE stg."__artie_delete" = true);`,
		RedshiftDialect{}.buildMergeDeleteQuery(
			fakeTableID,
			"{SUB_QUERY}",
			[]columns.Column{cols[0], cols[1]},
		),
	)
}

type result struct {
	PrimaryKeys []columns.Column
	Columns     []columns.Column
}

// getBasicColumnsForTest - will return you all the columns within `result` that are needed for tests.
// * In here, we'll return if compositeKey=false - id (pk), email, first_name, last_name, created_at, toast_text (TOAST-able)
// * Else if compositeKey=true - id(pk), email (pk), first_name, last_name, created_at, toast_text (TOAST-able)
func getBasicColumnsForTest(compositeKey bool) result {
	idCol := columns.NewColumn("id", typing.Float)
	emailCol := columns.NewColumn("email", typing.String)
	textToastCol := columns.NewColumn("toast_text", typing.String)
	textToastCol.ToastColumn = true

	var cols columns.Columns
	cols.AddColumn(idCol)
	cols.AddColumn(emailCol)
	cols.AddColumn(columns.NewColumn("first_name", typing.String))
	cols.AddColumn(columns.NewColumn("last_name", typing.String))
	cols.AddColumn(columns.NewColumn("created_at", typing.ETime))
	cols.AddColumn(textToastCol)
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	var pks []columns.Column
	pks = append(pks, idCol)

	if compositeKey {
		pks = append(pks, emailCol)
	}

	return result{
		PrimaryKeys: pks,
		Columns:     cols.ValidColumns(),
	}
}

func TestRedshiftDialect_BuildMergeQueries_SkipDelete(t *testing.T) {
	// Biggest difference with this test are:
	// 1. We are not saving `__artie_deleted` column
	// 2. There are 3 SQL queries (INSERT, UPDATE and DELETE)
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(false)

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")
	parts, err := RedshiftDialect{}.BuildMergeQueries(
		fakeTableID,
		tempTableName,
		"",
		res.PrimaryKeys,
		nil,
		res.Columns,
		false,
		false,
	)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(parts))

	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
		parts[0])

	assert.Equal(t,
		`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_delete", false) = false;`,
		parts[1])
}

func TestRedshiftDialect_BuildMergeQueries_SoftDelete(t *testing.T) {
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(false)

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")

	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			"",
			res.PrimaryKeys,
			nil,
			res.Columns,
			true,
			false,
		)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(parts))

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text",stg."__artie_delete" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
			parts[0])
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END,"__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id";`,
			parts[1])
	}
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			"created_at",
			res.PrimaryKeys,
			nil,
			res.Columns,
			true,
			false,
		)
		assert.NoError(t, err)

		// Parts[0] for insertion should be identical
		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text",stg."__artie_delete" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
			parts[0])
		// Parts[1] where we're doing UPDATES will have idempotency key.
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END,"__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND stg.created_at >= tgt.created_at;`,
			parts[1])
	}
}

func TestRedshiftDialect_BuildMergeQueries_SoftDeleteComposite(t *testing.T) {
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(true)
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			"",
			res.PrimaryKeys,
			nil,
			res.Columns,
			true,
			false,
		)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(parts))

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text",stg."__artie_delete" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" AND tgt."email" = stg."email" WHERE tgt."id" IS NULL;`,
			parts[0])
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END,"__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND tgt."email" = stg."email";`,
			parts[1])
	}
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			"created_at",
			res.PrimaryKeys,
			nil,
			res.Columns,
			true,
			false,
		)
		assert.NoError(t, err)

		// Parts[0] for insertion should be identical
		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text",stg."__artie_delete" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" AND tgt."email" = stg."email" WHERE tgt."id" IS NULL;`,
			parts[0])
		// Parts[1] where we're doing UPDATES will have idempotency key.
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END,"__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND tgt."email" = stg."email" AND stg.created_at >= tgt.created_at;`,
			parts[1])
	}
}

func TestRedshiftDialect_BuildMergeQueries(t *testing.T) {
	// Biggest difference with this test are:
	// 1. We are not saving `__artie_deleted` column
	// 2. There are 3 SQL queries (INSERT, UPDATE and DELETE)
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(false)
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			"",
			res.PrimaryKeys,
			nil,
			res.Columns,
			false,
			true,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
			parts[0])

		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_delete", false) = false;`,
			parts[1])

		assert.Equal(t,
			`DELETE FROM public.tableName WHERE ("id") IN (SELECT stg."id" FROM public.tableName__temp AS stg WHERE stg."__artie_delete" = true);`,
			parts[2])
	}
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			"created_at",
			res.PrimaryKeys,
			nil,
			res.Columns,
			false,
			true,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
			parts[0])

		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND stg.created_at >= tgt.created_at AND COALESCE(stg."__artie_delete", false) = false;`,
			parts[1])

		assert.Equal(t,
			`DELETE FROM public.tableName WHERE ("id") IN (SELECT stg."id" FROM public.tableName__temp AS stg WHERE stg."__artie_delete" = true);`,
			parts[2])
	}
}

func TestRedshiftDialect_BuildMergeQueries_CompositeKey(t *testing.T) {
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(true)
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			"",
			res.PrimaryKeys,
			nil,
			res.Columns,
			false,
			true,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" AND tgt."email" = stg."email" WHERE tgt."id" IS NULL;`,
			parts[0])

		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND tgt."email" = stg."email" AND COALESCE(stg."__artie_delete", false) = false;`,
			parts[1])

		assert.Equal(t,
			`DELETE FROM public.tableName WHERE ("id","email") IN (SELECT stg."id",stg."email" FROM public.tableName__temp AS stg WHERE stg."__artie_delete" = true);`,
			parts[2])
	}
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			"created_at",
			res.PrimaryKeys,
			nil,
			res.Columns,
			false,
			true,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" AND tgt."email" = stg."email" WHERE tgt."id" IS NULL;`,
			parts[0])

		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND tgt."email" = stg."email" AND stg.created_at >= tgt.created_at AND COALESCE(stg."__artie_delete", false) = false;`,
			parts[1])

		assert.Equal(t,
			`DELETE FROM public.tableName WHERE ("id","email") IN (SELECT stg."id",stg."email" FROM public.tableName__temp AS stg WHERE stg."__artie_delete" = true);`,
			parts[2])
	}
}
