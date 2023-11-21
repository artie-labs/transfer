package dml

import (
	"context"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func (m *MergeTestSuite) TestMergeStatementPartsValidation() {
	for _, arg := range []*MergeArgument{
		{DestKind: constants.Snowflake},
		{DestKind: constants.SnowflakeStages},
		{DestKind: constants.BigQuery},
	} {
		parts, err := MergeStatementParts(m.ctx, arg)
		assert.Error(m.T(), err)
		assert.Nil(m.T(), parts)
	}
}

type result struct {
	PrimaryKeys    []columns.Wrapper
	ColumnsToTypes columns.Columns
}

// getBasicColumnsForTest - will return you all the columns within `result` that are needed for tests.
// * In here, we'll return if compositeKey=false - id (pk), email, first_name, last_name, created_at, toast_text (TOAST-able)
// * Else if compositeKey=true - id(pk), email (pk), first_name, last_name, created_at, toast_text (TOAST-able)
func getBasicColumnsForTest(ctx context.Context, compositeKey bool) result {
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

	var pks []columns.Wrapper
	pks = append(pks, columns.NewWrapper(ctx, idCol, &sql.NameArgs{
		Escape:   true,
		DestKind: constants.Redshift,
	}))

	if compositeKey {
		pks = append(pks, columns.NewWrapper(ctx, emailCol, &sql.NameArgs{
			Escape:   true,
			DestKind: constants.Redshift,
		}))
	}

	return result{
		PrimaryKeys:    pks,
		ColumnsToTypes: cols,
	}
}

func (m *MergeTestSuite) TestMergeStatementParts_SkipDelete() {
	// Biggest difference with this test are:
	// 1. We are not saving `__artie_deleted` column
	// 2. There are 3 SQL queries (INSERT, UPDATE and DELETE)
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(m.ctx, false)
	mergeArg := &MergeArgument{
		FqTableName:    fqTableName,
		SubQuery:       tempTableName,
		PrimaryKeys:    res.PrimaryKeys,
		ColumnsToTypes: res.ColumnsToTypes,
		DestKind:       constants.Redshift,
		SkipDelete:     true,
	}

	parts, err := MergeStatementParts(m.ctx, mergeArg)
	assert.NoError(m.T(), err)
	assert.Equal(m.T(), 2, len(parts))

	assert.Equal(m.T(),
		`INSERT INTO public.tableName (id,email,first_name,last_name,created_at,toast_text) SELECT cc.id,cc.email,cc.first_name,cc.last_name,cc.created_at,cc.toast_text FROM public.tableName__temp as cc LEFT JOIN public.tableName as c on c.id = cc.id WHERE c.id IS NULL;`,
		parts[0])

	assert.Equal(m.T(),
		`UPDATE public.tableName as c SET id=cc.id,email=cc.email,first_name=cc.first_name,last_name=cc.last_name,created_at=cc.created_at,toast_text= CASE WHEN cc.toast_text != '__debezium_unavailable_value' THEN cc.toast_text ELSE c.toast_text END FROM public.tableName__temp as cc WHERE c.id = cc.id AND COALESCE(cc.__artie_delete, false) = false;`,
		parts[1])
}

func (m *MergeTestSuite) TestMergeStatementPartsSoftDelete() {
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(m.ctx, false)
	mergeArg := &MergeArgument{
		FqTableName:    fqTableName,
		SubQuery:       tempTableName,
		PrimaryKeys:    res.PrimaryKeys,
		ColumnsToTypes: res.ColumnsToTypes,
		DestKind:       constants.Redshift,
		SoftDelete:     true,
	}
	parts, err := MergeStatementParts(m.ctx, mergeArg)
	assert.NoError(m.T(), err)
	assert.Equal(m.T(), 2, len(parts))

	assert.Equal(m.T(),
		`INSERT INTO public.tableName (id,email,first_name,last_name,created_at,toast_text,__artie_delete) SELECT cc.id,cc.email,cc.first_name,cc.last_name,cc.created_at,cc.toast_text,cc.__artie_delete FROM public.tableName__temp as cc LEFT JOIN public.tableName as c on c.id = cc.id WHERE c.id IS NULL;`,
		parts[0])
	assert.Equal(m.T(),
		`UPDATE public.tableName as c SET id=cc.id,email=cc.email,first_name=cc.first_name,last_name=cc.last_name,created_at=cc.created_at,toast_text= CASE WHEN cc.toast_text != '__debezium_unavailable_value' THEN cc.toast_text ELSE c.toast_text END,__artie_delete=cc.__artie_delete FROM public.tableName__temp as cc WHERE c.id = cc.id;`,
		parts[1])

	mergeArg.IdempotentKey = "created_at"
	parts, err = MergeStatementParts(m.ctx, mergeArg)
	assert.NoError(m.T(), err)

	// Parts[0] for insertion should be identical
	assert.Equal(m.T(),
		`INSERT INTO public.tableName (id,email,first_name,last_name,created_at,toast_text,__artie_delete) SELECT cc.id,cc.email,cc.first_name,cc.last_name,cc.created_at,cc.toast_text,cc.__artie_delete FROM public.tableName__temp as cc LEFT JOIN public.tableName as c on c.id = cc.id WHERE c.id IS NULL;`,
		parts[0])
	// Parts[1] where we're doing UPDATES will have idempotency key.
	assert.Equal(m.T(),
		`UPDATE public.tableName as c SET id=cc.id,email=cc.email,first_name=cc.first_name,last_name=cc.last_name,created_at=cc.created_at,toast_text= CASE WHEN cc.toast_text != '__debezium_unavailable_value' THEN cc.toast_text ELSE c.toast_text END,__artie_delete=cc.__artie_delete FROM public.tableName__temp as cc WHERE c.id = cc.id AND cc.created_at >= c.created_at;`,
		parts[1])
}

func (m *MergeTestSuite) TestMergeStatementPartsSoftDeleteComposite() {
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(m.ctx, true)
	mergeArg := &MergeArgument{
		FqTableName:    fqTableName,
		SubQuery:       tempTableName,
		PrimaryKeys:    res.PrimaryKeys,
		ColumnsToTypes: res.ColumnsToTypes,
		DestKind:       constants.Redshift,
		SoftDelete:     true,
	}

	parts, err := MergeStatementParts(m.ctx, mergeArg)
	assert.NoError(m.T(), err)
	assert.Equal(m.T(), 2, len(parts))

	assert.Equal(m.T(),
		`INSERT INTO public.tableName (id,email,first_name,last_name,created_at,toast_text,__artie_delete) SELECT cc.id,cc.email,cc.first_name,cc.last_name,cc.created_at,cc.toast_text,cc.__artie_delete FROM public.tableName__temp as cc LEFT JOIN public.tableName as c on c.id = cc.id and c.email = cc.email WHERE c.id IS NULL;`,
		parts[0])
	assert.Equal(m.T(),
		`UPDATE public.tableName as c SET id=cc.id,email=cc.email,first_name=cc.first_name,last_name=cc.last_name,created_at=cc.created_at,toast_text= CASE WHEN cc.toast_text != '__debezium_unavailable_value' THEN cc.toast_text ELSE c.toast_text END,__artie_delete=cc.__artie_delete FROM public.tableName__temp as cc WHERE c.id = cc.id and c.email = cc.email;`,
		parts[1])

	mergeArg.IdempotentKey = "created_at"
	parts, err = MergeStatementParts(m.ctx, mergeArg)
	assert.NoError(m.T(), err)

	// Parts[0] for insertion should be identical
	assert.Equal(m.T(),
		`INSERT INTO public.tableName (id,email,first_name,last_name,created_at,toast_text,__artie_delete) SELECT cc.id,cc.email,cc.first_name,cc.last_name,cc.created_at,cc.toast_text,cc.__artie_delete FROM public.tableName__temp as cc LEFT JOIN public.tableName as c on c.id = cc.id and c.email = cc.email WHERE c.id IS NULL;`,
		parts[0])
	// Parts[1] where we're doing UPDATES will have idempotency key.
	assert.Equal(m.T(),
		`UPDATE public.tableName as c SET id=cc.id,email=cc.email,first_name=cc.first_name,last_name=cc.last_name,created_at=cc.created_at,toast_text= CASE WHEN cc.toast_text != '__debezium_unavailable_value' THEN cc.toast_text ELSE c.toast_text END,__artie_delete=cc.__artie_delete FROM public.tableName__temp as cc WHERE c.id = cc.id and c.email = cc.email AND cc.created_at >= c.created_at;`,
		parts[1])
}

func (m *MergeTestSuite) TestMergeStatementParts() {
	// Biggest difference with this test are:
	// 1. We are not saving `__artie_deleted` column
	// 2. There are 3 SQL queries (INSERT, UPDATE and DELETE)
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(m.ctx, false)
	mergeArg := &MergeArgument{
		FqTableName:    fqTableName,
		SubQuery:       tempTableName,
		PrimaryKeys:    res.PrimaryKeys,
		ColumnsToTypes: res.ColumnsToTypes,
		DestKind:       constants.Redshift,
	}

	parts, err := MergeStatementParts(m.ctx, mergeArg)
	assert.NoError(m.T(), err)
	assert.Equal(m.T(), 3, len(parts))

	assert.Equal(m.T(),
		`INSERT INTO public.tableName (id,email,first_name,last_name,created_at,toast_text) SELECT cc.id,cc.email,cc.first_name,cc.last_name,cc.created_at,cc.toast_text FROM public.tableName__temp as cc LEFT JOIN public.tableName as c on c.id = cc.id WHERE c.id IS NULL;`,
		parts[0])

	assert.Equal(m.T(),
		`UPDATE public.tableName as c SET id=cc.id,email=cc.email,first_name=cc.first_name,last_name=cc.last_name,created_at=cc.created_at,toast_text= CASE WHEN cc.toast_text != '__debezium_unavailable_value' THEN cc.toast_text ELSE c.toast_text END FROM public.tableName__temp as cc WHERE c.id = cc.id AND COALESCE(cc.__artie_delete, false) = false;`,
		parts[1])

	assert.Equal(m.T(),
		`DELETE FROM public.tableName WHERE (id) IN (SELECT cc.id FROM public.tableName__temp as cc WHERE cc.__artie_delete = true);`,
		parts[2])

	mergeArg = &MergeArgument{
		FqTableName:    fqTableName,
		SubQuery:       tempTableName,
		PrimaryKeys:    res.PrimaryKeys,
		ColumnsToTypes: res.ColumnsToTypes,
		DestKind:       constants.Redshift,
		IdempotentKey:  "created_at",
	}

	parts, err = MergeStatementParts(m.ctx, mergeArg)
	assert.NoError(m.T(), err)
	assert.Equal(m.T(), 3, len(parts))

	assert.Equal(m.T(),
		`INSERT INTO public.tableName (id,email,first_name,last_name,created_at,toast_text) SELECT cc.id,cc.email,cc.first_name,cc.last_name,cc.created_at,cc.toast_text FROM public.tableName__temp as cc LEFT JOIN public.tableName as c on c.id = cc.id WHERE c.id IS NULL;`,
		parts[0])

	assert.Equal(m.T(),
		`UPDATE public.tableName as c SET id=cc.id,email=cc.email,first_name=cc.first_name,last_name=cc.last_name,created_at=cc.created_at,toast_text= CASE WHEN cc.toast_text != '__debezium_unavailable_value' THEN cc.toast_text ELSE c.toast_text END FROM public.tableName__temp as cc WHERE c.id = cc.id AND cc.created_at >= c.created_at AND COALESCE(cc.__artie_delete, false) = false;`,
		parts[1])

	assert.Equal(m.T(),
		`DELETE FROM public.tableName WHERE (id) IN (SELECT cc.id FROM public.tableName__temp as cc WHERE cc.__artie_delete = true);`,
		parts[2])
}

func (m *MergeTestSuite) TestMergeStatementPartsCompositeKey() {
	fqTableName := "public.tableName"
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(m.ctx, true)
	mergeArg := &MergeArgument{
		FqTableName:    fqTableName,
		SubQuery:       tempTableName,
		PrimaryKeys:    res.PrimaryKeys,
		ColumnsToTypes: res.ColumnsToTypes,
		DestKind:       constants.Redshift,
	}

	parts, err := MergeStatementParts(m.ctx, mergeArg)
	assert.NoError(m.T(), err)
	assert.Equal(m.T(), 3, len(parts))

	assert.Equal(m.T(),
		`INSERT INTO public.tableName (id,email,first_name,last_name,created_at,toast_text) SELECT cc.id,cc.email,cc.first_name,cc.last_name,cc.created_at,cc.toast_text FROM public.tableName__temp as cc LEFT JOIN public.tableName as c on c.id = cc.id and c.email = cc.email WHERE c.id IS NULL;`,
		parts[0])

	assert.Equal(m.T(),
		`UPDATE public.tableName as c SET id=cc.id,email=cc.email,first_name=cc.first_name,last_name=cc.last_name,created_at=cc.created_at,toast_text= CASE WHEN cc.toast_text != '__debezium_unavailable_value' THEN cc.toast_text ELSE c.toast_text END FROM public.tableName__temp as cc WHERE c.id = cc.id and c.email = cc.email AND COALESCE(cc.__artie_delete, false) = false;`,
		parts[1])

	assert.Equal(m.T(),
		`DELETE FROM public.tableName WHERE (id,email) IN (SELECT cc.id,cc.email FROM public.tableName__temp as cc WHERE cc.__artie_delete = true);`,
		parts[2])

	mergeArg = &MergeArgument{
		FqTableName:    fqTableName,
		SubQuery:       tempTableName,
		PrimaryKeys:    res.PrimaryKeys,
		ColumnsToTypes: res.ColumnsToTypes,
		DestKind:       constants.Redshift,
		IdempotentKey:  "created_at",
	}

	parts, err = MergeStatementParts(m.ctx, mergeArg)
	assert.NoError(m.T(), err)
	assert.Equal(m.T(), 3, len(parts))

	assert.Equal(m.T(),
		`INSERT INTO public.tableName (id,email,first_name,last_name,created_at,toast_text) SELECT cc.id,cc.email,cc.first_name,cc.last_name,cc.created_at,cc.toast_text FROM public.tableName__temp as cc LEFT JOIN public.tableName as c on c.id = cc.id and c.email = cc.email WHERE c.id IS NULL;`,
		parts[0])

	assert.Equal(m.T(),
		`UPDATE public.tableName as c SET id=cc.id,email=cc.email,first_name=cc.first_name,last_name=cc.last_name,created_at=cc.created_at,toast_text= CASE WHEN cc.toast_text != '__debezium_unavailable_value' THEN cc.toast_text ELSE c.toast_text END FROM public.tableName__temp as cc WHERE c.id = cc.id and c.email = cc.email AND cc.created_at >= c.created_at AND COALESCE(cc.__artie_delete, false) = false;`,
		parts[1])

	assert.Equal(m.T(),
		`DELETE FROM public.tableName WHERE (id,email) IN (SELECT cc.id,cc.email FROM public.tableName__temp as cc WHERE cc.__artie_delete = true);`,
		parts[2])
}
