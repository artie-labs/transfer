package columns

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	redshiftDialect "github.com/artie-labs/transfer/clients/redshift/dialect"
	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

var dialects = []sql.Dialect{
	bigQueryDialect.BigQueryDialect{},
	redshiftDialect.RedshiftDialect{},
	snowflakeDialect.SnowflakeDialect{},
}

func TestColumn_DefaultValue(t *testing.T) {
	birthday := time.Date(2022, time.September, 6, 3, 19, 24, 942000000, time.UTC)
	birthdayExtDateTime, err := ext.ParseExtendedDateTime(birthday.Format(ext.ISO8601), nil)
	assert.NoError(t, err)

	// date
	dateKind := typing.ETime
	dateKind.ExtendedTimeDetails = &ext.Date
	// time
	timeKind := typing.ETime
	timeKind.ExtendedTimeDetails = &ext.Time
	// date time
	dateTimeKind := typing.ETime
	dateTimeKind.ExtendedTimeDetails = &ext.DateTime

	testCases := []struct {
		name                       string
		col                        *Column
		dialect                    sql.Dialect
		expectedValue              any
		destKindToExpectedValueMap map[sql.Dialect]any
	}{
		{
			name: "default value = nil",
			col: &Column{
				KindDetails:  typing.String,
				defaultValue: nil,
			},
			expectedValue: nil,
		},
		{
			name: "string",
			col: &Column{
				KindDetails:  typing.String,
				defaultValue: "abcdef",
			},
			expectedValue: "'abcdef'",
		},
		{
			name: "json",
			col: &Column{
				KindDetails:  typing.Struct,
				defaultValue: "{}",
			},
			expectedValue: `{}`,
			destKindToExpectedValueMap: map[sql.Dialect]any{
				dialects[0]: "JSON'{}'",
				dialects[1]: `JSON_PARSE('{}')`,
				dialects[2]: `'{}'`,
			},
		},
		{
			name: "json w/ some values",
			col: &Column{
				KindDetails:  typing.Struct,
				defaultValue: "{\"age\": 0, \"membership_level\": \"standard\"}",
			},
			expectedValue: "{\"age\": 0, \"membership_level\": \"standard\"}",
			destKindToExpectedValueMap: map[sql.Dialect]any{
				dialects[0]: "JSON'{\"age\": 0, \"membership_level\": \"standard\"}'",
				dialects[1]: "JSON_PARSE('{\"age\": 0, \"membership_level\": \"standard\"}')",
				dialects[2]: "'{\"age\": 0, \"membership_level\": \"standard\"}'",
			},
		},
		{
			name: "date",
			col: &Column{
				KindDetails:  dateKind,
				defaultValue: birthdayExtDateTime,
			},
			expectedValue: "'2022-09-06'",
		},
		{
			name: "time",
			col: &Column{
				KindDetails:  timeKind,
				defaultValue: birthdayExtDateTime,
			},
			expectedValue: "'03:19:24.942'",
		},
		{
			name: "datetime",
			col: &Column{
				KindDetails:  dateTimeKind,
				defaultValue: birthdayExtDateTime,
			},
			expectedValue: "'2022-09-06T03:19:24.942Z'",
		},
	}

	for _, testCase := range testCases {
		for _, dialect := range dialects {
			actualValue, actualErr := testCase.col.DefaultValue(dialect, nil)
			assert.NoError(t, actualErr, fmt.Sprintf("%s %s", testCase.name, dialect))

			expectedValue := testCase.expectedValue
			if potentialValue, isOk := testCase.destKindToExpectedValueMap[dialect]; isOk {
				// Not everything requires a destination specific value, so only use this if necessary.
				expectedValue = potentialValue
			}

			assert.Equal(t, expectedValue, actualValue, fmt.Sprintf("%s %s", testCase.name, dialect))
		}
	}
}
