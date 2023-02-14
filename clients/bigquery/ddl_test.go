package bigquery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
)

func (b *BigQueryTestSuite) TestAlterTableDropColumns() {
	fqName := "mock_dataset.delete_col"
	ctx := context.Background()
	ts := time.Now()
	columns := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}

	originalColumnLength := len(columns)
	b.store.configMap.AddTableToConfig(fqName, types.NewDwhTableConfig(columns, nil, false))

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(b.T(), 0, len(b.store.configMap.TableConfig(fqName).ColumnsToDelete()), b.store.configMap.TableConfig(fqName).ColumnsToDelete())
	for name, kind := range columns {
		err := b.store.alterTable(ctx, fqName, b.store.configMap.TableConfig(fqName).CreateTable, constants.Delete, ts,
			typing.Column{
				Name: name,
				Kind: kind,
			})

		assert.NoError(b.T(), err)
	}

	// Have not deleted, but tried to!
	assert.Equal(b.T(), originalColumnLength, len(b.store.configMap.TableConfig(fqName).ColumnsToDelete()), b.store.configMap.TableConfig(fqName).ColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(b.T(), originalColumnLength, len(b.store.configMap.TableConfig(fqName).Columns()), b.store.configMap.TableConfig(fqName).Columns())

	// Now try to delete again and with an increased TS. It should now be all deleted.
	var callIdx int
	for name, kind := range columns {
		err := b.store.alterTable(ctx, fqName, b.store.configMap.TableConfig(fqName).CreateTable, constants.Delete, ts.Add(2*constants.DeletionConfidencePadding),
			typing.Column{
				Name: name,
				Kind: kind,
			})

		query, _ := b.fakeStore.ExecArgsForCall(callIdx)
		assert.Equal(b.T(), fmt.Sprintf("ALTER TABLE %s drop COLUMN %s", fqName, name), query)
		assert.NoError(b.T(), err)
		callIdx += 1
	}

	// Columns have now been deleted.
	assert.Equal(b.T(), 0, len(b.store.configMap.TableConfig(fqName).ColumnsToDelete()), b.store.configMap.TableConfig(fqName).ColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(b.T(), 0, len(b.store.configMap.TableConfig(fqName).Columns()), b.store.configMap.TableConfig(fqName).Columns())
	assert.Equal(b.T(), originalColumnLength, b.fakeStore.ExecCallCount())
}

func (b *BigQueryTestSuite) TestAlterTableAddColumns() {
	fqName := "mock_dataset.add_cols"
	ctx := context.Background()
	ts := time.Now()
	existingCols := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}
	existingColsLen := len(existingCols)

	newCols := map[string]typing.KindDetails{
		"dusty":      typing.String,
		"jacqueline": typing.Integer,
		"charlie":    typing.Boolean,
		"robin":      typing.Float,
	}
	newColsLen := len(newCols)

	b.store.configMap.AddTableToConfig(fqName, types.NewDwhTableConfig(existingCols, nil, false))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(b.T(), 0, len(b.store.configMap.TableConfig(fqName).ColumnsToDelete()), b.store.configMap.TableConfig(fqName).ColumnsToDelete())
	assert.Equal(b.T(), len(existingCols), len(b.store.configMap.TableConfig(fqName).Columns()), b.store.configMap.TableConfig(fqName).Columns())

	var callIdx int
	for name, kind := range newCols {
		err := b.store.alterTable(ctx, fqName, b.store.configMap.TableConfig(fqName).CreateTable, constants.Add, ts,
			typing.Column{
				Name: name,
				Kind: kind,
			})

		assert.NoError(b.T(), err)
		query, _ := b.fakeStore.ExecArgsForCall(callIdx)
		assert.Equal(b.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, name, typing.KindToBigQuery(kind)), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(b.T(), newColsLen+existingColsLen, len(b.store.configMap.TableConfig(fqName).Columns()), b.store.configMap.TableConfig(fqName).Columns())
	// Check by iterating over the columns
	for tableCol, tableColKind := range b.store.configMap.TableConfig(fqName).Columns() {
		var isOk bool
		var kind typing.KindDetails
		kind, isOk = existingCols[tableCol]
		if !isOk {
			kind, isOk = newCols[tableCol]
		}

		assert.Equal(b.T(), tableColKind, kind)
		assert.True(b.T(), isOk)
	}
}

func (b *BigQueryTestSuite) TestAlterTableAddColumnsSomeAlreadyExist() {
	fqName := "mock_dataset.add_cols"
	ctx := context.Background()
	ts := time.Now()
	existingCols := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}
	existingColsLen := len(existingCols)

	b.store.configMap.AddTableToConfig(fqName, types.NewDwhTableConfig(existingCols, nil, false))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(b.T(), 0, len(b.store.configMap.TableConfig(fqName).ColumnsToDelete()), b.store.configMap.TableConfig(fqName).ColumnsToDelete())
	assert.Equal(b.T(), len(existingCols), len(b.store.configMap.TableConfig(fqName).Columns()), b.store.configMap.TableConfig(fqName).Columns())

	var callIdx int
	for name, kind := range existingCols {
		var sqlResult sql.Result
		// BQ returning the same error because the column already exists.
		b.fakeStore.ExecReturnsOnCall(0, sqlResult, errors.New("Column already exists: _string at [1:39]"))
		err := b.store.alterTable(ctx, fqName, b.store.configMap.TableConfig(fqName).CreateTable, constants.Add, ts,
			typing.Column{
				Name: name,
				Kind: kind,
			})

		assert.NoError(b.T(), err)
		query, _ := b.fakeStore.ExecArgsForCall(callIdx)
		assert.Equal(b.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, name, typing.KindToBigQuery(kind)), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(b.T(), existingColsLen, len(b.store.configMap.TableConfig(fqName).Columns()), b.store.configMap.TableConfig(fqName).Columns())
	// Check by iterating over the columns
	for tableCol, tableColKind := range b.store.configMap.TableConfig(fqName).Columns() {
		kind, isOk := existingCols[tableCol]
		assert.Equal(b.T(), tableColKind, kind)
		assert.True(b.T(), isOk)
	}
}

func (b *BigQueryTestSuite) TestAlterTableCreateTable() {
	fqName := "mock_dataset.mock_table"
	b.store.configMap.AddTableToConfig(fqName, types.NewDwhTableConfig(nil, nil, true))

	ctx := context.Background()
	err := b.store.alterTable(ctx, fqName, b.store.configMap.TableConfig(fqName).CreateTable,
		constants.Add, time.Time{}, typing.Column{Name: "name", Kind: typing.String})

	assert.Equal(b.T(), 1, b.fakeStore.ExecCallCount())

	query, _ := b.fakeStore.ExecArgsForCall(0)
	assert.Equal(b.T(), query, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (name string)", fqName), query)
	assert.NoError(b.T(), err, err)
	assert.Equal(b.T(), false, b.store.configMap.TableConfig(fqName).CreateTable)
}

func (b *BigQueryTestSuite) TestParseSchemaQuery() {
	basicQueries := []string{
		"CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING) OPTIONS(expiration_timestamp=TIMESTAMP);",
		"CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING)OPTIONS(expiration_timestamp=TIMESTAMP);", // No spacing
		"CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING);",                                        // No OPTIONS
	}

	for _, basicQuery := range basicQueries {
		tableConfig, err := parseSchemaQuery(basicQuery, false)

		assert.NoError(b.T(), err, err)

		assert.Equal(b.T(), len(tableConfig.Columns()), 2, tableConfig.Columns)
		for col, kind := range tableConfig.Columns() {
			assert.Equal(b.T(), kind, typing.String, fmt.Sprintf("col: %s, kind: %v incorrect", col, kind))
		}
	}
}

func (b *BigQueryTestSuite) TestParseSchemaQueryComplex() {
	// This test will test every single data type.
	tableConfig, err := parseSchemaQuery("CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING,field2 INT64,field3 ARRAY<INT64>,field4 FLOAT64,field5 NUMERIC,field6 BIGNUMERIC,field7 BOOL,field8 TIMESTAMP,field9 DATE,field10 TIME,field11 DATETIME,field12 STRUCT<foo STRING>,field13 JSON, field14 TIME)OPTIONS(expiration_timestamp=TIMESTAMP 2023-03-26T20:03:44.504Z);",
		false)

	assert.NoError(b.T(), err, err)
	assert.Equal(b.T(), len(tableConfig.Columns()), 15, tableConfig.Columns)

	anticipatedColumns := map[string]typing.KindDetails{
		"string_field_0": typing.String,
		"string_field_1": typing.String,
		"field2":         typing.Integer,
		"field3":         typing.Array,
		"field4":         typing.Float,
		"field5":         typing.Float,
		"field6":         typing.Float,
		"field7":         typing.Boolean,
		"field8":         typing.NewKindDetailsFromTemplate(typing.ETime, typing.DateTimeKindType),
		"field9":         typing.NewKindDetailsFromTemplate(typing.ETime, typing.DateKindType),
		"field10":        typing.NewKindDetailsFromTemplate(typing.ETime, typing.TimeKindType),
		"field11":        typing.NewKindDetailsFromTemplate(typing.ETime, typing.DateTimeKindType),
		"field12":        typing.Struct,
		"field13":        typing.Struct,
		"field14":        typing.NewKindDetailsFromTemplate(typing.ETime, typing.TimeKindType),
	}

	for anticipatedCol, anticipatedKind := range anticipatedColumns {
		kindDetails, isOk := tableConfig.Columns()[anticipatedCol]
		assert.True(b.T(), isOk)
		assert.Equal(b.T(), kindDetails.Kind, anticipatedKind.Kind, fmt.Sprintf("expected kind: %v, got: col: %s, kind: %v mismatched.", kindDetails.Kind,
			anticipatedCol, anticipatedKind))

		if kindDetails.Kind == typing.ETime.Kind {
			assert.Equal(b.T(), kindDetails.ExtendedTimeDetails.Type, anticipatedKind.ExtendedTimeDetails.Type)
		}

	}
}
