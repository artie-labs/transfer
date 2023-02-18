package bigquery

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func (b *BigQueryTestSuite) TestParseSchemaQuery() {
	basicQueries := []string{
		"CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING) OPTIONS(expiration_timestamp=TIMESTAMP);",
		"CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING)OPTIONS(expiration_timestamp=TIMESTAMP);", // No spacing
		"CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING);",                                        // No OPTIONS
	}

	for _, basicQuery := range basicQueries {
		tableConfig, err := parseSchemaQuery(basicQuery, false, true)

		assert.NoError(b.T(), err, err)

		assert.Equal(b.T(), true, tableConfig.DropDeletedColumns())
		assert.Equal(b.T(), len(tableConfig.Columns()), 2, tableConfig.Columns)
		for col, kind := range tableConfig.Columns() {
			assert.Equal(b.T(), kind, typing.String, fmt.Sprintf("col: %s, kind: %v incorrect", col, kind))
		}
	}
}

func (b *BigQueryTestSuite) TestParseSchemaQueryComplex() {
	// This test will test every single data type.
	tableConfig, err := parseSchemaQuery("CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING,field2 INT64,field3 ARRAY<INT64>,field4 FLOAT64,field5 NUMERIC,field6 BIGNUMERIC,field7 BOOL,field8 TIMESTAMP,field9 DATE,field10 TIME,field11 DATETIME,field12 STRUCT<foo STRING>,field13 JSON, field14 TIME)OPTIONS(expiration_timestamp=TIMESTAMP 2023-03-26T20:03:44.504Z);",
		false, false)

	assert.NoError(b.T(), err, err)
	assert.Equal(b.T(), false, tableConfig.DropDeletedColumns())
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
		"field8":         typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"field9":         typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		"field10":        typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		"field11":        typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"field12":        typing.Struct,
		"field13":        typing.Struct,
		"field14":        typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
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
