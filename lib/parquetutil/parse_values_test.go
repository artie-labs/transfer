package parquetutil

import (
	"math/big"
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

func TestParseValue(t *testing.T) {
	type _testStruct struct {
		name    string
		colVal  interface{}
		colKind columns.Column

		expectErr     bool
		expectedValue interface{}
	}

	eDecimal := typing.EDecimal
	eDecimal.ExtendedDecimalDetails = decimal.NewDecimal(5, ptr.ToInt(30), nil)

	eTime := typing.ETime
	eTime.ExtendedTimeDetails = &ext.Time

	eDate := typing.ETime
	eDate.ExtendedTimeDetails = &ext.Date

	eDateTime := typing.ETime
	eDateTime.ExtendedTimeDetails = &ext.DateTime

	testCases := []_testStruct{
		{
			name:          "nil value",
			colVal:        nil,
			expectedValue: nil,
		},
		{
			name:          "string value",
			colVal:        "test",
			colKind:       columns.NewColumn("", typing.String),
			expectedValue: "test",
		},
		{
			name: "struct value",
			colVal: map[string]interface{}{
				"foo": "bar",
			},
			colKind:       columns.NewColumn("", typing.Struct),
			expectedValue: `{"foo":"bar"}`,
		},
		{
			name:          "array (numbers - converted to string)",
			colVal:        []interface{}{123, 456},
			colKind:       columns.NewColumn("", typing.Array),
			expectedValue: []string{"123", "456"},
		},
		{
			name:          "array (boolean - converted to string)",
			colVal:        []interface{}{true, false, true},
			colKind:       columns.NewColumn("", typing.Array),
			expectedValue: []string{"true", "false", "true"},
		},
		{
			name:          "decimal",
			colVal:        decimal.NewDecimal(5, ptr.ToInt(30), big.NewFloat(5000.2232)),
			colKind:       columns.NewColumn("", eDecimal),
			expectedValue: "5000.22320",
		},
		{
			name:          "time",
			colVal:        "03:15:00",
			colKind:       columns.NewColumn("", eTime),
			expectedValue: "03:15:00+00",
		},
		{
			name:          "date",
			colVal:        "2022-12-25",
			colKind:       columns.NewColumn("", eDate),
			expectedValue: "2022-12-25",
		},
		{
			name:          "datetime",
			colVal:        "2023-04-24T17:29:05.69944Z",
			colKind:       columns.NewColumn("", eDateTime),
			expectedValue: int64(1682357345699),
		},
	}

	for _, tc := range testCases {
		actualValue, actualErr := ParseValue(tc.colVal, tc.colKind, nil)
		if tc.expectErr {
			assert.Error(t, actualErr, tc.name)
		} else {
			assert.NoError(t, actualErr, tc.name)
			assert.Equal(t, tc.expectedValue, actualValue, tc.name)
		}
	}
}
