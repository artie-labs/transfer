package bigquery

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"time"
)

func (b *BigQueryTestSuite) TestAlterTableDropColumns() {
	// TODO
}

func (b *BigQueryTestSuite) TestAlterTableAddColumns() {
	// TODO
}

func (b *BigQueryTestSuite) TestAlterTableAddColumnsSomeAlreadyExist() {
	// TODO
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
		tableConfig, err := ParseSchemaQuery(basicQuery, false)

		assert.NoError(b.T(), err, err)

		assert.Equal(b.T(), len(tableConfig.Columns()), 2, tableConfig.Columns)
		for col, kind := range tableConfig.Columns() {
			assert.Equal(b.T(), kind, typing.String, fmt.Sprintf("col: %s, kind: %v incorrect", col, kind))
		}
	}
}

func (b *BigQueryTestSuite) TestParseSchemaQueryComplex() {
	// This test will test every single data type.
	tableConfig, err := ParseSchemaQuery("CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING,field2 INT64,field3 ARRAY<INT64>,field4 FLOAT64,field5 NUMERIC,field6 BIGNUMERIC,field7 BOOL,field8 TIMESTAMP,field9 DATE,field10 TIME,field11 DATETIME,field12 STRUCT<foo STRING>,field13 JSON)OPTIONS(expiration_timestamp=TIMESTAMP 2023-03-26T20:03:44.504Z);",
		false)

	assert.NoError(b.T(), err, err)
	assert.Equal(b.T(), len(tableConfig.Columns()), 14, tableConfig.Columns)

	// TODO test and iterate over every column

}
