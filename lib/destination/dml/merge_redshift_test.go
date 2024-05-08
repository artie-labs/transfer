package dml

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

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

func TestMergeArgument_BuildStatements_Redshift(t *testing.T) {
	res := getBasicColumnsForTest(false)
	mergeArg := &MergeArgument{
		TableID:             MockTableIdentifier{"public.tableName"},
		SubQuery:            "{SUB_QUERY}",
		PrimaryKeys:         res.PrimaryKeys,
		Columns:             res.Columns,
		Dialect:             sql.RedshiftDialect{},
		ContainsHardDeletes: ptr.ToBool(true),
	}

	parts, err := mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(parts))
	parts2, err := mergeArg.BuildStatements()
	assert.NoError(t, err)
	assert.Equal(t, parts, parts2)
}

func TestMergeArgument_BuildRedshiftStatements_SkipDelete(t *testing.T) {
	// Biggest difference with this test are:
	// 1. We are not saving `__artie_deleted` column
	// 2. There are 3 SQL queries (INSERT, UPDATE and DELETE)
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(false)
	mergeArg := &MergeArgument{
		TableID:             MockTableIdentifier{fqTableName},
		SubQuery:            tempTableName,
		PrimaryKeys:         res.PrimaryKeys,
		Columns:             res.Columns,
		Dialect:             sql.RedshiftDialect{},
		ContainsHardDeletes: ptr.ToBool(false),
	}

	parts, err := mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(parts))

	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT cc."id",cc."email",cc."first_name",cc."last_name",cc."created_at",cc."toast_text" FROM public.tableName__temp AS cc LEFT JOIN public.tableName AS c ON c."id" = cc."id" WHERE c."id" IS NULL;`,
		parts[0])

	assert.Equal(t,
		`UPDATE public.tableName AS c SET "id"=cc."id","email"=cc."email","first_name"=cc."first_name","last_name"=cc."last_name","created_at"=cc."created_at","toast_text"= CASE WHEN COALESCE(cc."toast_text" != '__debezium_unavailable_value', true) THEN cc."toast_text" ELSE c."toast_text" END FROM public.tableName__temp AS cc WHERE c."id" = cc."id" AND COALESCE(cc."__artie_delete", false) = false;`,
		parts[1])
}

func TestMergeArgument_BuildRedshiftStatements_SoftDelete(t *testing.T) {
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(false)
	mergeArg := &MergeArgument{
		TableID:             MockTableIdentifier{fqTableName},
		SubQuery:            tempTableName,
		PrimaryKeys:         res.PrimaryKeys,
		Columns:             res.Columns,
		Dialect:             sql.RedshiftDialect{},
		SoftDelete:          true,
		ContainsHardDeletes: ptr.ToBool(false),
	}

	parts, err := mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(parts))

	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT cc."id",cc."email",cc."first_name",cc."last_name",cc."created_at",cc."toast_text",cc."__artie_delete" FROM public.tableName__temp AS cc LEFT JOIN public.tableName AS c ON c."id" = cc."id" WHERE c."id" IS NULL;`,
		parts[0])
	assert.Equal(t,
		`UPDATE public.tableName AS c SET "id"=cc."id","email"=cc."email","first_name"=cc."first_name","last_name"=cc."last_name","created_at"=cc."created_at","toast_text"= CASE WHEN COALESCE(cc."toast_text" != '__debezium_unavailable_value', true) THEN cc."toast_text" ELSE c."toast_text" END,"__artie_delete"=cc."__artie_delete" FROM public.tableName__temp AS cc WHERE c."id" = cc."id";`,
		parts[1])

	mergeArg.IdempotentKey = "created_at"
	parts, err = mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)

	// Parts[0] for insertion should be identical
	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT cc."id",cc."email",cc."first_name",cc."last_name",cc."created_at",cc."toast_text",cc."__artie_delete" FROM public.tableName__temp AS cc LEFT JOIN public.tableName AS c ON c."id" = cc."id" WHERE c."id" IS NULL;`,
		parts[0])
	// Parts[1] where we're doing UPDATES will have idempotency key.
	assert.Equal(t,
		`UPDATE public.tableName AS c SET "id"=cc."id","email"=cc."email","first_name"=cc."first_name","last_name"=cc."last_name","created_at"=cc."created_at","toast_text"= CASE WHEN COALESCE(cc."toast_text" != '__debezium_unavailable_value', true) THEN cc."toast_text" ELSE c."toast_text" END,"__artie_delete"=cc."__artie_delete" FROM public.tableName__temp AS cc WHERE c."id" = cc."id" AND cc.created_at >= c.created_at;`,
		parts[1])
}

func TestMergeArgument_BuildRedshiftStatements_SoftDeleteComposite(t *testing.T) {
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(true)
	mergeArg := &MergeArgument{
		TableID:             MockTableIdentifier{fqTableName},
		SubQuery:            tempTableName,
		PrimaryKeys:         res.PrimaryKeys,
		Columns:             res.Columns,
		Dialect:             sql.RedshiftDialect{},
		SoftDelete:          true,
		ContainsHardDeletes: ptr.ToBool(false),
	}

	parts, err := mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(parts))

	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT cc."id",cc."email",cc."first_name",cc."last_name",cc."created_at",cc."toast_text",cc."__artie_delete" FROM public.tableName__temp AS cc LEFT JOIN public.tableName AS c ON c."id" = cc."id" AND c."email" = cc."email" WHERE c."id" IS NULL;`,
		parts[0])
	assert.Equal(t,
		`UPDATE public.tableName AS c SET "id"=cc."id","email"=cc."email","first_name"=cc."first_name","last_name"=cc."last_name","created_at"=cc."created_at","toast_text"= CASE WHEN COALESCE(cc."toast_text" != '__debezium_unavailable_value', true) THEN cc."toast_text" ELSE c."toast_text" END,"__artie_delete"=cc."__artie_delete" FROM public.tableName__temp AS cc WHERE c."id" = cc."id" AND c."email" = cc."email";`,
		parts[1])

	mergeArg.IdempotentKey = "created_at"
	parts, err = mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)

	// Parts[0] for insertion should be identical
	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT cc."id",cc."email",cc."first_name",cc."last_name",cc."created_at",cc."toast_text",cc."__artie_delete" FROM public.tableName__temp AS cc LEFT JOIN public.tableName AS c ON c."id" = cc."id" AND c."email" = cc."email" WHERE c."id" IS NULL;`,
		parts[0])
	// Parts[1] where we're doing UPDATES will have idempotency key.
	assert.Equal(t,
		`UPDATE public.tableName AS c SET "id"=cc."id","email"=cc."email","first_name"=cc."first_name","last_name"=cc."last_name","created_at"=cc."created_at","toast_text"= CASE WHEN COALESCE(cc."toast_text" != '__debezium_unavailable_value', true) THEN cc."toast_text" ELSE c."toast_text" END,"__artie_delete"=cc."__artie_delete" FROM public.tableName__temp AS cc WHERE c."id" = cc."id" AND c."email" = cc."email" AND cc.created_at >= c.created_at;`,
		parts[1])
}

func TestMergeArgument_GetRedshiftStatements(t *testing.T) {
	// Biggest difference with this test are:
	// 1. We are not saving `__artie_deleted` column
	// 2. There are 3 SQL queries (INSERT, UPDATE and DELETE)
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(false)
	mergeArg := &MergeArgument{
		TableID:             MockTableIdentifier{fqTableName},
		SubQuery:            tempTableName,
		PrimaryKeys:         res.PrimaryKeys,
		Columns:             res.Columns,
		Dialect:             sql.RedshiftDialect{},
		ContainsHardDeletes: ptr.ToBool(true),
	}

	parts, err := mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(parts))

	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT cc."id",cc."email",cc."first_name",cc."last_name",cc."created_at",cc."toast_text" FROM public.tableName__temp AS cc LEFT JOIN public.tableName AS c ON c."id" = cc."id" WHERE c."id" IS NULL;`,
		parts[0])

	assert.Equal(t,
		`UPDATE public.tableName AS c SET "id"=cc."id","email"=cc."email","first_name"=cc."first_name","last_name"=cc."last_name","created_at"=cc."created_at","toast_text"= CASE WHEN COALESCE(cc."toast_text" != '__debezium_unavailable_value', true) THEN cc."toast_text" ELSE c."toast_text" END FROM public.tableName__temp AS cc WHERE c."id" = cc."id" AND COALESCE(cc."__artie_delete", false) = false;`,
		parts[1])

	assert.Equal(t,
		`DELETE FROM public.tableName WHERE ("id") IN (SELECT cc."id" FROM public.tableName__temp AS cc WHERE cc."__artie_delete" = true);`,
		parts[2])

	mergeArg = &MergeArgument{
		TableID:             MockTableIdentifier{fqTableName},
		SubQuery:            tempTableName,
		PrimaryKeys:         res.PrimaryKeys,
		Columns:             res.Columns,
		Dialect:             sql.RedshiftDialect{},
		IdempotentKey:       "created_at",
		ContainsHardDeletes: ptr.ToBool(true),
	}

	parts, err = mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(parts))

	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT cc."id",cc."email",cc."first_name",cc."last_name",cc."created_at",cc."toast_text" FROM public.tableName__temp AS cc LEFT JOIN public.tableName AS c ON c."id" = cc."id" WHERE c."id" IS NULL;`,
		parts[0])

	assert.Equal(t,
		`UPDATE public.tableName AS c SET "id"=cc."id","email"=cc."email","first_name"=cc."first_name","last_name"=cc."last_name","created_at"=cc."created_at","toast_text"= CASE WHEN COALESCE(cc."toast_text" != '__debezium_unavailable_value', true) THEN cc."toast_text" ELSE c."toast_text" END FROM public.tableName__temp AS cc WHERE c."id" = cc."id" AND cc.created_at >= c.created_at AND COALESCE(cc."__artie_delete", false) = false;`,
		parts[1])

	assert.Equal(t,
		`DELETE FROM public.tableName WHERE ("id") IN (SELECT cc."id" FROM public.tableName__temp AS cc WHERE cc."__artie_delete" = true);`,
		parts[2])
}

func TestMergeArgument_BuildRedshiftStatements_CompositeKey(t *testing.T) {
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(true)
	mergeArg := &MergeArgument{
		TableID:             MockTableIdentifier{fqTableName},
		SubQuery:            tempTableName,
		PrimaryKeys:         res.PrimaryKeys,
		Columns:             res.Columns,
		Dialect:             sql.RedshiftDialect{},
		ContainsHardDeletes: ptr.ToBool(true),
	}

	parts, err := mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(parts))

	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT cc."id",cc."email",cc."first_name",cc."last_name",cc."created_at",cc."toast_text" FROM public.tableName__temp AS cc LEFT JOIN public.tableName AS c ON c."id" = cc."id" AND c."email" = cc."email" WHERE c."id" IS NULL;`,
		parts[0])

	assert.Equal(t,
		`UPDATE public.tableName AS c SET "id"=cc."id","email"=cc."email","first_name"=cc."first_name","last_name"=cc."last_name","created_at"=cc."created_at","toast_text"= CASE WHEN COALESCE(cc."toast_text" != '__debezium_unavailable_value', true) THEN cc."toast_text" ELSE c."toast_text" END FROM public.tableName__temp AS cc WHERE c."id" = cc."id" AND c."email" = cc."email" AND COALESCE(cc."__artie_delete", false) = false;`,
		parts[1])

	assert.Equal(t,
		`DELETE FROM public.tableName WHERE ("id","email") IN (SELECT cc."id",cc."email" FROM public.tableName__temp AS cc WHERE cc."__artie_delete" = true);`,
		parts[2])

	mergeArg = &MergeArgument{
		TableID:             MockTableIdentifier{fqTableName},
		SubQuery:            tempTableName,
		PrimaryKeys:         res.PrimaryKeys,
		Columns:             res.Columns,
		Dialect:             sql.RedshiftDialect{},
		ContainsHardDeletes: ptr.ToBool(true),
		IdempotentKey:       "created_at",
	}

	parts, err = mergeArg.buildRedshiftStatements()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(parts))

	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT cc."id",cc."email",cc."first_name",cc."last_name",cc."created_at",cc."toast_text" FROM public.tableName__temp AS cc LEFT JOIN public.tableName AS c ON c."id" = cc."id" AND c."email" = cc."email" WHERE c."id" IS NULL;`,
		parts[0])

	assert.Equal(t,
		`UPDATE public.tableName AS c SET "id"=cc."id","email"=cc."email","first_name"=cc."first_name","last_name"=cc."last_name","created_at"=cc."created_at","toast_text"= CASE WHEN COALESCE(cc."toast_text" != '__debezium_unavailable_value', true) THEN cc."toast_text" ELSE c."toast_text" END FROM public.tableName__temp AS cc WHERE c."id" = cc."id" AND c."email" = cc."email" AND cc.created_at >= c.created_at AND COALESCE(cc."__artie_delete", false) = false;`,
		parts[1])

	assert.Equal(t,
		`DELETE FROM public.tableName WHERE ("id","email") IN (SELECT cc."id",cc."email" FROM public.tableName__temp AS cc WHERE cc."__artie_delete" = true);`,
		parts[2])
}
