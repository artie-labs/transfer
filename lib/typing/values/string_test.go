package values

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type _testCase struct {
	name    string
	colVal  interface{}
	colKind columns.Column

	errorMessage   string
	expectedString string
}

func evaluateTestCase(t *testing.T, testCase _testCase) {
	actualString, actualErr := ToString(testCase.colVal, testCase.colKind, nil)
	if len(testCase.errorMessage) > 0 {
		assert.ErrorContains(t, actualErr, testCase.errorMessage, testCase.name)
	} else {
		assert.NoError(t, actualErr, testCase.name)
		assert.Equal(t, testCase.expectedString, actualString, testCase.name)
	}
}

func TestCastColValStaging_Basic(t *testing.T) {
	testCases := []_testCase{
		{
			name:         "null value",
			colVal:       nil,
			errorMessage: "colVal is nil",
		},
		{
			name:   "colKind = string, colVal = JSON (this happens because of schema inference)",
			colVal: map[string]interface{}{"hello": "world"},
			colKind: columns.Column{
				KindDetails: typing.String,
			},

			expectedString: `{"hello":"world"}`,
		},
		{
			name:   "empty string",
			colVal: "",
			colKind: columns.Column{
				KindDetails: typing.String,
			},

			expectedString: "",
		},
		{
			name:   "string",
			colVal: "foo",
			colKind: columns.Column{
				KindDetails: typing.String,
			},

			expectedString: "foo",
		},
		{
			name:   "integer",
			colVal: 7,
			colKind: columns.Column{
				KindDetails: typing.Integer,
			},
			expectedString: "7",
		},
		{
			name:   "boolean",
			colVal: true,
			colKind: columns.Column{
				KindDetails: typing.Boolean,
			},
			expectedString: "true",
		},
		{
			name:   "array",
			colVal: []string{"hello", "there"},
			colKind: columns.Column{
				KindDetails: typing.Array,
			},
			expectedString: `["hello","there"]`,
		},
		{
			name:   "JSON string",
			colVal: `{"hello": "world"}`,
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedString: `{"hello": "world"}`,
		},
		{
			name:   "JSON struct",
			colVal: map[string]string{"hello": "world"},
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedString: `{"hello":"world"}`,
		},
		{
			name:   "numeric data types (backwards compatibility)",
			colVal: decimal.NewDecimal(2, ptr.ToInt(5), big.NewFloat(55.22)),
			colKind: columns.Column{
				KindDetails: typing.Float,
			},

			expectedString: "55.22",
		},
		// Indigestion stuff
		{
			name:   "numeric data types (string)",
			colVal: "123.45",
			colKind: columns.Column{
				KindDetails: typing.EDecimal,
			},
			expectedString: "123.45",
		},
		{
			name:   "numeric data types (float)",
			colVal: 123.45,
			colKind: columns.Column{
				KindDetails: typing.EDecimal,
			},
			expectedString: "123.45",
		},
		{
			name:   "numeric data types",
			colVal: decimal.NewDecimal(2, ptr.ToInt(38), big.NewFloat(585692791691858.25)),
			colKind: columns.Column{
				KindDetails: typing.EDecimal,
			},
			expectedString: "585692791691858.25",
		},
	}

	for _, testCase := range testCases {
		evaluateTestCase(t, testCase)
	}
}

func TestCastColValStaging_Array(t *testing.T) {
	testCases := []_testCase{
		{
			name:   "array w/ numbers - but type is STRING",
			colVal: []int{1, 2, 3, 4, 5},
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedString: `[1,2,3,4,5]`,
		},
		{
			name:   "array w/ numbers",
			colVal: []int{1, 2, 3, 4, 5},
			colKind: columns.Column{
				KindDetails: typing.Array,
			},
			expectedString: `[1,2,3,4,5]`,
		},
		{
			name: "array w/ nested objects (JSON)",
			colKind: columns.Column{
				KindDetails: typing.Array,
			},
			colVal: []map[string]interface{}{
				{
					"dusty": "the mini aussie",
				},
				{
					"robin": "tang",
				},
				{
					"foo": "bar",
				},
			},
			expectedString: `[{"dusty":"the mini aussie"},{"robin":"tang"},{"foo":"bar"}]`,
		},
		{
			name: "array w/ bools",
			colKind: columns.Column{
				KindDetails: typing.Array,
			},
			colVal: []bool{
				true,
				true,
				false,
				false,
				true,
			},
			expectedString: `[true,true,false,false,true]`,
		},
	}

	for _, testCase := range testCases {
		evaluateTestCase(t, testCase)
	}
}

// TestCastColValStaging_Time - will test all the variants of date, time and date time.
func TestCastColValStaging_Time(t *testing.T) {
	birthday := time.Date(2022, time.September, 6, 3, 19, 24, 942000000, time.UTC)
	// date
	dateKind := typing.ETime
	dateKind.ExtendedTimeDetails = &ext.Date
	// time
	timeKind := typing.ETime
	timeKind.ExtendedTimeDetails = &ext.Time
	// date time
	dateTimeKind := typing.ETime
	dateTimeKind.ExtendedTimeDetails = &ext.DateTime

	birthdate, err := ext.NewExtendedTime(birthday, dateKind.ExtendedTimeDetails.Type, "")
	assert.NoError(t, err)

	birthTime, err := ext.NewExtendedTime(birthday, timeKind.ExtendedTimeDetails.Type, "")
	assert.NoError(t, err)

	birthDateTime, err := ext.NewExtendedTime(birthday, dateTimeKind.ExtendedTimeDetails.Type, "")
	assert.NoError(t, err)

	testCases := []_testCase{
		{
			name:   "date",
			colVal: birthdate,
			colKind: columns.Column{
				KindDetails: dateKind,
			},
			expectedString: "2022-09-06",
		},
		{
			name:   "date (but value is dateTime)",
			colVal: birthDateTime,
			colKind: columns.Column{
				KindDetails: dateKind,
			},
			expectedString: "2022-09-06",
		},
		{
			name:   "time",
			colVal: birthTime,
			colKind: columns.Column{
				KindDetails: timeKind,
			},
			expectedString: "03:19:24.942",
		},
		{
			name:   "datetime",
			colVal: birthDateTime,
			colKind: columns.Column{
				KindDetails: dateTimeKind,
			},
			expectedString: "2022-09-06T03:19:24.942Z",
		},
	}

	for _, testCase := range testCases {
		evaluateTestCase(t, testCase)
	}
}

func TestCastColValStaging_TOAST(t *testing.T) {
	// Toast only really matters for JSON blobs since it'll return a STRING value that's not a JSON object.
	// We're testing that we're casting the unavailable value correctly into a JSON object so that it can compile.
	testCases := []_testCase{
		{
			name:   "struct with TOAST value",
			colVal: constants.ToastUnavailableValuePlaceholder,
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedString: fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder),
		},
	}

	for _, testCase := range testCases {
		evaluateTestCase(t, testCase)
	}
}
