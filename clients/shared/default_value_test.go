package shared

import (
	"fmt"
	"testing"
	"time"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	redshiftDialect "github.com/artie-labs/transfer/clients/redshift/dialect"
	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

var dialects = []sql.Dialect{
	bigQueryDialect.BigQueryDialect{},
	redshiftDialect.RedshiftDialect{},
	snowflakeDialect.SnowflakeDialect{},
}

func TestColumn_DefaultValue(t *testing.T) {
	birthday := time.Date(2022, time.September, 6, 3, 19, 24, 942000000, time.UTC)
	{
		// nil
		col := columns.NewColumnWithDefaultValue("", typing.String, nil)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr)
			assert.Nil(t, actualValue)
		}
	}
	{
		// String
		col := columns.NewColumnWithDefaultValue("", typing.String, "abcdef")
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr)
			assert.Equal(t, "'abcdef'", actualValue)
		}
	}
	{
		// JSON (empty)
		col := columns.NewColumnWithDefaultValue("", typing.Struct, map[string]any{})
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr)
			var expectedValue string
			switch dialect.(type) {
			case bigQueryDialect.BigQueryDialect:
				expectedValue = "JSON'{}'"
			case redshiftDialect.RedshiftDialect:
				expectedValue = `JSON_PARSE('{}')`
			case snowflakeDialect.SnowflakeDialect:
				expectedValue = `'{}'`
			}
			assert.Equal(t, expectedValue, actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}
	{
		// JSON (with values)
		jsonMap := map[string]any{"age": 0, "membership_level": "standard"}
		col := columns.NewColumnWithDefaultValue("", typing.Struct, jsonMap)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))

			var expectedValue string
			switch dialect.(type) {
			case bigQueryDialect.BigQueryDialect:
				expectedValue = `JSON'{"age":0,"membership_level":"standard"}'`
			case redshiftDialect.RedshiftDialect:
				expectedValue = `JSON_PARSE('{"age":0,"membership_level":"standard"}')`
			case snowflakeDialect.SnowflakeDialect:
				expectedValue = `'{"age":0,"membership_level":"standard"}'`
			}
			assert.Equal(t, expectedValue, actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}
	{
		// DATE
		col := columns.NewColumnWithDefaultValue("", typing.Date, birthday)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))
			assert.Equal(t, "'2022-09-06'", actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}
	{
		// TIMESTAMP_NTZ
		col := columns.NewColumnWithDefaultValue("", typing.TimestampNTZ, birthday)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))
			assert.Equal(t, "'2022-09-06T03:19:24.942'", actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}
	{
		// TIME
		col := columns.NewColumnWithDefaultValue("", typing.Time, birthday)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))
			assert.Equal(t, "'03:19:24.942'", actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}
	{
		// TIMESTAMP_TZ
		col := columns.NewColumnWithDefaultValue("", typing.TimestampTZ, birthday)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))
			assert.Equal(t, "'2022-09-06T03:19:24.942Z'", actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}
	{
		// decimal.Decimal
		decimalValue := decimal.NewDecimal(numbers.MustParseDecimal("3.14159"))
		col := columns.NewColumnWithDefaultValue("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(7, 5)), decimalValue)
		value, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
		assert.NoError(t, err)
		assert.Equal(t, "3.14159", value)
	}
	{
		// Column is a decimal, however the incoming data is an integer.
		col := columns.NewColumnWithDefaultValue("", typing.EDecimal, int64(123))
		value, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
		assert.NoError(t, err)
		assert.Equal(t, "123", value)
	}
	{
		// Int64
		col := columns.NewColumnWithDefaultValue("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)), int64(123))
		value, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
		assert.NoError(t, err)
		assert.Equal(t, "123", value)
	}
	{
		// Wrong data type
		col := columns.NewColumnWithDefaultValue("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(7, 5)), "hello")
		_, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
		assert.ErrorContains(t, err, "expected type *decimal.Decimal, got string")
	}
}
