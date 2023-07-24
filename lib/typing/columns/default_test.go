package columns

import (
	"time"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/stretchr/testify/assert"
)

func (c *ColumnsTestSuite) TestColumn_DefaultValue() {
	type _testCase struct {
		name          string
		col           *Column
		args          *DefaultValueArgs
		expectedValue interface{}
		expectedEr    bool
	}

	birthday := time.Date(2022, time.September, 6, 3, 19, 24, 942000000, time.UTC)
	birthdayExtDateTime, err := ext.ParseExtendedDateTime(c.ctx, birthday.Format(ext.ISO8601))
	assert.NoError(c.T(), err)

	// date
	dateKind := typing.ETime
	dateKind.ExtendedTimeDetails = &ext.Date
	// time
	timeKind := typing.ETime
	timeKind.ExtendedTimeDetails = &ext.Time
	// date time
	dateTimeKind := typing.ETime
	dateTimeKind.ExtendedTimeDetails = &ext.DateTime

	testCases := []_testCase{
		{
			name: "escaped args (nil)",
			col: &Column{
				KindDetails:  typing.String,
				defaultValue: "abcdef",
			},
			expectedValue: "abcdef",
		},
		{
			name: "escaped args (escaped = false)",
			col: &Column{
				KindDetails:  typing.String,
				defaultValue: "abcdef",
			},
			args:          &DefaultValueArgs{},
			expectedValue: "abcdef",
		},
		{
			name: "string",
			col: &Column{
				KindDetails:  typing.String,
				defaultValue: "abcdef",
			},
			args: &DefaultValueArgs{
				Escape: true,
			},
			expectedValue: "'abcdef'",
		},
		{
			name: "json",
			col: &Column{
				KindDetails:  typing.Struct,
				defaultValue: "{}",
			},
			args: &DefaultValueArgs{
				Escape: true,
			},
			expectedValue: "{}",
		},
		{
			name: "json (bigquery)",
			col: &Column{
				KindDetails:  typing.Struct,
				defaultValue: "{}",
			},
			args: &DefaultValueArgs{
				Escape:   true,
				DestKind: constants.BigQuery,
			},
			expectedValue: "JSON'{}'",
		},
		{
			name: "json (redshift)",
			col: &Column{
				KindDetails:  typing.Struct,
				defaultValue: "{}",
			},
			args: &DefaultValueArgs{
				Escape:   true,
				DestKind: constants.Redshift,
			},
			expectedValue: "'{}'",
		},
		{
			name: "date",
			col: &Column{
				KindDetails:  dateKind,
				defaultValue: birthdayExtDateTime,
			},
			args: &DefaultValueArgs{
				Escape: true,
			},
			expectedValue: "'2022-09-06'",
		},
		{
			name: "time",
			col: &Column{
				KindDetails:  timeKind,
				defaultValue: birthdayExtDateTime,
			},
			args: &DefaultValueArgs{
				Escape: true,
			},
			expectedValue: "'03:19:24'",
		},
		{
			name: "datetime",
			col: &Column{
				KindDetails:  dateTimeKind,
				defaultValue: birthdayExtDateTime,
			},
			args: &DefaultValueArgs{
				Escape: true,
			},
			expectedValue: "'2022-09-06T03:19:24Z'",
		},
	}

	for _, testCase := range testCases {
		actualValue, actualErr := testCase.col.DefaultValue(c.ctx, testCase.args)
		if testCase.expectedEr {
			assert.Error(c.T(), actualErr, testCase.name)
		} else {
			assert.NoError(c.T(), actualErr, testCase.name)
		}

		assert.Equal(c.T(), testCase.expectedValue, actualValue, testCase.name)
	}
}
