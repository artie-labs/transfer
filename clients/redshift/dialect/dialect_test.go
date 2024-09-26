package dialect

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func TestRedshiftDialect_QuoteIdentifier(t *testing.T) {
	dialect := RedshiftDialect{}
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("FOO"))
}

func TestRedshiftDialect_DataTypeForKind(t *testing.T) {
	{
		// String
		{
			assert.Equal(t, "VARCHAR(MAX)", RedshiftDialect{}.DataTypeForKind(typing.String, true))
		}
		{
			assert.Equal(t, "VARCHAR(12345)", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(12345))}, false))
		}
	}
	{
		// Integers
		{
			// Small int
			assert.Equal(t, "INT2", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind)}, false))
		}
		{
			// Integer
			assert.Equal(t, "INT4", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, false))
		}
		{
			// Big integer
			assert.Equal(t, "INT8", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, false))
		}
		{
			// Not specified
			{
				// Literal
				assert.Equal(t, "INT8", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.NotSpecifiedKind)}, false))
			}
			{
				assert.Equal(t, "INT8", RedshiftDialect{}.DataTypeForKind(typing.Integer, false))
			}
		}
	}
}

func TestRedshiftDialect_KindForDataType(t *testing.T) {
	dialect := RedshiftDialect{}
	{
		// Integers
		{
			// Small integer
			kd, err := dialect.KindForDataType("smallint", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind)}, kd)
		}
		{
			{
				// Regular integers (upper)
				kd, err := dialect.KindForDataType("INTEGER", "")
				assert.NoError(t, err)
				assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, kd)
			}
			{
				// Regular integers (lower)
				kd, err := dialect.KindForDataType("integer", "")
				assert.NoError(t, err)
				assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, kd)
			}
		}
		{
			// Big integer
			kd, err := dialect.KindForDataType("bigint", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, kd)
		}
	}
	{
		// Double
		{
			kd, err := dialect.KindForDataType("double precision", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Float, kd)
		}
		{
			kd, err := dialect.KindForDataType("DOUBLE precision", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Float, kd)
		}
	}
	{
		// Numeric
		{
			kd, err := dialect.KindForDataType("numeric(5,2)", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
		}
		{
			kd, err := dialect.KindForDataType("numeric(5,5)", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Scale())
		}
	}
	{
		// Boolean
		kd, err := dialect.KindForDataType("boolean", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		// String with precision
		kd, err := dialect.KindForDataType("character varying", "65535")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(65535))}, kd)
	}
	{
		// Times
		{
			kd, err := dialect.KindForDataType("timestamp with time zone", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.ETime.Kind, kd.Kind)
			assert.Equal(t, ext.TimestampTzKindType, kd.ExtendedTimeDetails.Type)
		}
		{
			kd, err := dialect.KindForDataType("timestamp without time zone", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.ETime.Kind, kd.Kind)
			assert.Equal(t, ext.TimestampTzKindType, kd.ExtendedTimeDetails.Type)
		}
		{
			kd, err := dialect.KindForDataType("time without time zone", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.ETime.Kind, kd.Kind)
			assert.Equal(t, ext.TimeKindType, kd.ExtendedTimeDetails.Type)
		}
		{
			kd, err := dialect.KindForDataType("date", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.ETime.Kind, kd.Kind)
			assert.Equal(t, ext.DateKindType, kd.ExtendedTimeDetails.Type)
		}
	}
}

func TestRedshiftDialect_IsColumnAlreadyExistsErr(t *testing.T) {
	{
		// Irrelevant error
		assert.False(t, RedshiftDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf("foo")))
	}
	{
		// Actual error
		assert.True(t, RedshiftDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf(`ERROR: column "foo" of relation "statement" already exists [ErrorId: 1-64da9ea9]`)))
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

func TestRedshiftDialect_BuildIncreaseStringPrecisionQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		`ALTER TABLE {TABLE} ALTER COLUMN "{column}" TYPE VARCHAR(12345)`,
		RedshiftDialect{}.BuildIncreaseStringPrecisionQuery(fakeTableID, columns.NewColumn("{COLUMN}", typing.String), 12345),
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
		name       string
		softDelete bool
		expected   []string
	}{
		{
			name:       "soft delete enabled",
			softDelete: true,
			expected: []string{
				`UPDATE {TABLE_ID} AS tgt SET "col1"=stg."col1","col2"=stg."col2","col3"=stg."col3" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" AND COALESCE(stg."__artie_only_set_delete", false) = false;`,
				`UPDATE {TABLE_ID} AS tgt SET "__artie_delete"=stg."__artie_delete" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" AND COALESCE(stg."__artie_only_set_delete", false) = true;`,
			},
		},
		{
			name:       "soft delete disabled",
			softDelete: false,
			expected: []string{
				`UPDATE {TABLE_ID} AS tgt SET "col1"=stg."col1","col2"=stg."col2","col3"=stg."col3" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" AND COALESCE(stg."__artie_delete", false) = false;`,
			},
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
		actual := RedshiftDialect{}.buildMergeUpdateQueries(
			fakeTableID,
			"{SUB_QUERY}",
			[]columns.Column{cols[0], cols[2]},
			cols,
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
	cols.AddColumn(columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean))

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
			res.PrimaryKeys,
			nil,
			res.Columns,
			true,
			false,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text",stg."__artie_delete" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
			parts[0])
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END,"__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_only_set_delete", false) = false;`,
			parts[1])
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_only_set_delete", false) = true;`,
			parts[2])
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
			res.PrimaryKeys,
			nil,
			res.Columns,
			true,
			false,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text",stg."__artie_delete" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" AND tgt."email" = stg."email" WHERE tgt."id" IS NULL;`,
			parts[0])
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" != '__debezium_unavailable_value', true) THEN stg."toast_text" ELSE tgt."toast_text" END,"__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND tgt."email" = stg."email" AND COALESCE(stg."__artie_only_set_delete", false) = false;`,
			parts[1])
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND tgt."email" = stg."email" AND COALESCE(stg."__artie_only_set_delete", false) = true;`,
			parts[2])
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
}
