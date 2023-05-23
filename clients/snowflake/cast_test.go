package snowflake

import (
	"fmt"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

type _testCase struct {
	name    string
	colVal  interface{}
	colKind typing.Column

	expectedString string
	expectErr      bool
}

func evaluateTestCase(t *testing.T, testCase _testCase) {
	actualString, actualErr := CastColValStaging(testCase.colVal, testCase.colKind)
	if testCase.expectErr {
		assert.Error(t, actualErr, testCase.name)
	} else {
		assert.NoError(t, actualErr, testCase.name)
	}

	fmt.Println("actualString", actualString, testCase.name)
	assert.Equal(t, testCase.expectedString, actualString, testCase.name)
}

func (s *SnowflakeTestSuite) TestCastColValStaging_Basic() {
	testCases := []_testCase{
		{
			name:   "string",
			colVal: "foo",
			colKind: typing.Column{
				KindDetails: typing.String,
			},

			expectedString: "foo",
		},
		{
			name:   "integer",
			colVal: 7,
			colKind: typing.Column{
				KindDetails: typing.Integer,
			},
			expectedString: "7",
		},
		{
			name:   "boolean",
			colVal: true,
			colKind: typing.Column{
				KindDetails: typing.Boolean,
			},
			expectedString: "true",
		},
		{
			name:   "array",
			colVal: []string{"hello", "there"},
			colKind: typing.Column{
				KindDetails: typing.Array,
			},
			expectedString: `["hello","there"]`,
		},
		{
			name:   "JSON string",
			colVal: `{"hello": "world"}`,
			colKind: typing.Column{
				KindDetails: typing.Struct,
			},
			expectedString: `{"hello": "world"}`,
		},
		{
			name:   "JSON struct",
			colVal: map[string]string{"hello": "world"},
			colKind: typing.Column{
				KindDetails: typing.Struct,
			},
			expectedString: `{"hello":"world"}`,
		},
	}

	for _, testCase := range testCases {
		evaluateTestCase(s.T(), testCase)
	}
}

func (s *SnowflakeTestSuite) TestCastColValStaging_Array() {
	testCases := []_testCase{
		{
			name:   "array w/ numbers",
			colVal: []int{1, 2, 3, 4, 5},
			colKind: typing.Column{
				KindDetails: typing.Array,
			},
			expectedString: `[1,2,3,4,5]`,
		},
		{
			name: "array w/ nested objects (JSON)",
			colKind: typing.Column{
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
			colKind: typing.Column{
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
		evaluateTestCase(s.T(), testCase)
	}
}

// TestCastColValStaging_Time - will test all the variants of date, time and date time.
func (s *SnowflakeTestSuite) TestCastColValStaging_Time() {
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
	assert.NoError(s.T(), err)

	birthTime, err := ext.NewExtendedTime(birthday, timeKind.ExtendedTimeDetails.Type, "")
	assert.NoError(s.T(), err)

	birthDateTime, err := ext.NewExtendedTime(birthday, dateTimeKind.ExtendedTimeDetails.Type, "")
	assert.NoError(s.T(), err)

	testCases := []_testCase{
		{
			name:   "date",
			colVal: birthdate,
			colKind: typing.Column{
				KindDetails: dateKind,
			},
			expectedString: "2022-09-06",
		},
		{
			name:   "time",
			colVal: birthTime,
			colKind: typing.Column{
				KindDetails: timeKind,
			},
			expectedString: "03:19:24.942",
		},
		{
			name:   "datetime",
			colVal: birthDateTime,
			colKind: typing.Column{
				KindDetails: dateTimeKind,
			},
			expectedString: "2022-09-06T03:19:24.942Z",
		},
	}

	for _, testCase := range testCases {
		evaluateTestCase(s.T(), testCase)
	}
}

func (s *SnowflakeTestSuite) TestCastColValStaging_TOAST() {
	// Toast only really matters for JSON blobs since it'll return a STRING value that's not a JSON object.
	// We're testing that we're casting the unavailable value correctly into a JSON object so that it can compile.
	testCases := []_testCase{
		{
			name:   "struct with TOAST value",
			colVal: constants.ToastUnavailableValuePlaceholder,
			colKind: typing.Column{
				KindDetails: typing.Struct,
			},
			expectedString: fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder),
		},
	}

	for _, testCase := range testCases {
		evaluateTestCase(s.T(), testCase)
	}
}
