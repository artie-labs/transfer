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
		// Testing nil default value handling across dialects
		col := columns.NewColumnWithDefaultValue("", typing.String, nil)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr)
			assert.Nil(t, actualValue)
		}
	}
	{
		// Testing string default value handling across dialects
		col := columns.NewColumnWithDefaultValue("", typing.String, "abcdef")
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr)
			assert.Equal(t, "'abcdef'", actualValue)
		}
	}
	{
		// Testing empty JSON default value handling across all dialects
		col := columns.NewColumnWithDefaultValue("", typing.Struct, "{}")
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
		// Testing JSON with values default value handling across all dialects
		jsonStr := `{"age": 0, "membership_level": "standard"}`
		col := columns.NewColumnWithDefaultValue("", typing.Struct, jsonStr)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))

			var expectedValue string
			switch dialect.(type) {
			case bigQueryDialect.BigQueryDialect:
				expectedValue = "JSON'" + jsonStr + "'"
			case redshiftDialect.RedshiftDialect:
				expectedValue = "JSON_PARSE('" + jsonStr + "')"
			case snowflakeDialect.SnowflakeDialect:
				expectedValue = "'" + jsonStr + "'"
			}
			assert.Equal(t, expectedValue, actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}

	{
		// Testing date default value handling across all dialects
		col := columns.NewColumnWithDefaultValue("", typing.Date, birthday)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))
			assert.Equal(t, "'2022-09-06'", actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}

	{
		// Testing timestamp_ntz default value handling across all dialects
		col := columns.NewColumnWithDefaultValue("", typing.TimestampNTZ, birthday)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))
			assert.Equal(t, "'2022-09-06T03:19:24.942'", actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}
	{
		// Testing time default value handling across all dialects
		col := columns.NewColumnWithDefaultValue("", typing.Time, birthday)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))
			assert.Equal(t, "'03:19:24.942'", actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}
	{
		// Testing timestamp_tz default value handling across all dialects
		col := columns.NewColumnWithDefaultValue("", typing.TimestampTZ, birthday)
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("dialect: %v", dialect))
			assert.Equal(t, "'2022-09-06T03:19:24.942Z'", actualValue, fmt.Sprintf("dialect: %v", dialect))
		}
	}
	{
		// Testing decimal default value handling with different types
		{
			// Testing decimal.Decimal type
			decimalValue := decimal.NewDecimal(numbers.MustParseDecimal("3.14159"))
			col := columns.NewColumnWithDefaultValue("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(7, 5)), decimalValue)
			value, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
			assert.NoError(t, err)
			assert.Equal(t, "3.14159", value)
		}
		{
			// Testing int64 type
			col := columns.NewColumnWithDefaultValue("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)), int64(123))
			value, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
			assert.NoError(t, err)
			assert.Equal(t, "123", value)
		}
		{
			// Testing wrong type (string) error case
			col := columns.NewColumnWithDefaultValue("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(7, 5)), "hello")
			_, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
			assert.ErrorContains(t, err, "expected type *decimal.Decimal, got string")
		}
	}
}
