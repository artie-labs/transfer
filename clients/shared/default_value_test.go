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
	testCases := []struct {
		name                       string
		col                        columns.Column
		dialect                    sql.Dialect
		expectedValue              any
		destKindToExpectedValueMap map[sql.Dialect]any
	}{
		{
			name:          "default value = nil",
			col:           columns.NewColumnWithDefaultValue("", typing.String, nil),
			expectedValue: nil,
		},
		{
			name:          "string",
			col:           columns.NewColumnWithDefaultValue("", typing.String, "abcdef"),
			expectedValue: "'abcdef'",
		},
		{
			name:          "json",
			col:           columns.NewColumnWithDefaultValue("", typing.Struct, "{}"),
			expectedValue: `{}`,
			destKindToExpectedValueMap: map[sql.Dialect]any{
				dialects[0]: "JSON'{}'",
				dialects[1]: `JSON_PARSE('{}')`,
				dialects[2]: `'{}'`,
			},
		},
		{
			name:          "json w/ some values",
			col:           columns.NewColumnWithDefaultValue("", typing.Struct, "{\"age\": 0, \"membership_level\": \"standard\"}"),
			expectedValue: "{\"age\": 0, \"membership_level\": \"standard\"}",
			destKindToExpectedValueMap: map[sql.Dialect]any{
				dialects[0]: "JSON'{\"age\": 0, \"membership_level\": \"standard\"}'",
				dialects[1]: "JSON_PARSE('{\"age\": 0, \"membership_level\": \"standard\"}')",
				dialects[2]: "'{\"age\": 0, \"membership_level\": \"standard\"}'",
			},
		},
		{
			name:          "date",
			col:           columns.NewColumnWithDefaultValue("", typing.Date, birthday),
			expectedValue: "'2022-09-06'",
		},
		{
			name:          "timestamp_ntz",
			col:           columns.NewColumnWithDefaultValue("", typing.TimestampNTZ, birthday),
			expectedValue: "'2022-09-06T03:19:24.942'",
		},
		{
			name:          "time",
			col:           columns.NewColumnWithDefaultValue("", typing.Time, birthday),
			expectedValue: "'03:19:24.942'",
		},
		{
			name:          "timestamp_tz",
			col:           columns.NewColumnWithDefaultValue("", typing.TimestampTZ, birthday),
			expectedValue: "'2022-09-06T03:19:24.942Z'",
		},
	}

	for _, testCase := range testCases {
		for _, dialect := range dialects {
			actualValue, actualErr := DefaultValue(testCase.col, dialect)
			assert.NoError(t, actualErr, fmt.Sprintf("%s %s", testCase.name, dialect))

			expectedValue := testCase.expectedValue
			if potentialValue, isOk := testCase.destKindToExpectedValueMap[dialect]; isOk {
				// Not everything requires a destination specific value, so only use this if necessary.
				expectedValue = potentialValue
			}

			assert.Equal(t, expectedValue, actualValue, fmt.Sprintf("%s %s", testCase.name, dialect))
		}
	}
	{
		// Decimal value
		{
			// Type *decimal.Decimal
			decimalValue := decimal.NewDecimal(numbers.MustParseDecimal("3.14159"))
			col := columns.NewColumnWithDefaultValue("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(7, 5)), decimalValue)
			value, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
			assert.NoError(t, err)
			assert.Equal(t, "3.14159", value)
		}
		{
			// Type int64
			col := columns.NewColumnWithDefaultValue("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)), int64(123))
			value, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
			assert.NoError(t, err)
			assert.Equal(t, "123", value)
		}
		{
			// Wrong type (string)
			col := columns.NewColumnWithDefaultValue("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(7, 5)), "hello")
			_, err := DefaultValue(col, redshiftDialect.RedshiftDialect{})
			assert.ErrorContains(t, err, "expected type *decimal.Decimal, got string")
		}
	}
}
