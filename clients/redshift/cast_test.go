package redshift

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/db"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (r *RedshiftTestSuite) TestReplaceExceededValues() {
	type _tc struct {
		name           string
		colVal         string
		colKind        columns.Column
		expectedResult string
	}

	tcs := []_tc{
		{
			name:   "string",
			colVal: stringutil.Random(maxRedshiftVarCharLen + 1),
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedResult: constants.ExceededValueMarker,
		},
		{
			name:   "string - not masked",
			colVal: "thisissuperlongbutnotlongenoughtogetmasked",
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedResult: "thisissuperlongbutnotlongenoughtogetmasked",
		},
		{
			name:   "struct",
			colVal: fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(maxRedshiftSuperLen+1)),
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedResult: fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker),
		},
		{
			name:   "struct - not masked",
			colVal: `{"foo": "bar"}`,
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedResult: `{"foo": "bar"}`,
		},
	}

	for _, tc := range tcs {
		assert.Equal(r.T(), tc.expectedResult, replaceExceededValues(tc.colVal, tc.colKind), tc.name)
	}
}

type _testCase struct {
	name    string
	colVal  interface{}
	colKind columns.Column

	expectedString string
	expectErr      bool
}

func evaluateTestCase(t *testing.T, store *Store, testCase _testCase) {
	actualString, actualErr := store.CastColValStaging(testCase.colVal, testCase.colKind, nil)
	if testCase.expectErr {
		assert.Error(t, actualErr, testCase.name)
	} else {
		assert.NoError(t, actualErr, testCase.name)
	}
	assert.Equal(t, testCase.expectedString, actualString, testCase.name)
}

func (r *RedshiftTestSuite) TestCastColValStaging_Basic() {
	testCases := []_testCase{
		{
			name:   "float",
			colVal: float32(15333599),
			colKind: columns.Column{
				KindDetails: typing.Integer,
			},
			expectedString: "15333599",
		},
		{
			name:   "float",
			colVal: 1533358,
			colKind: columns.Column{
				KindDetails: typing.Integer,
			},
			expectedString: "1533358",
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
			name:   "null value (string, not that it matters)",
			colVal: nil,
			colKind: columns.Column{
				KindDetails: typing.String,
			},

			expectedString: `\N`,
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
			name:   "array (string with interface type)",
			colVal: []interface{}{"hello", "there", "world"},
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedString: `["hello","there","world"]`,
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
		{
			name:   "numeric data types (float)",
			colVal: 123.45,
			colKind: columns.Column{
				KindDetails: typing.EDecimal,
			},
			expectedString: "123.45",
		},
		{
			name:   "numeric data types (string)",
			colVal: "123.45",
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
		evaluateTestCase(r.T(), r.store, testCase)
	}
}

func (r *RedshiftTestSuite) TestCastColValStaging_Array() {
	testCases := []_testCase{
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
		{
			name: "json object, but this is inferred as a string",
			colVal: map[string]interface{}{
				"foo": "bar",
			},
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedString: `{"foo":"bar"}`,
		},
		{
			name: "list of json object, but this is inferred as a string",
			colVal: []map[string]interface{}{
				{
					"foo": "bar",
				},
				{
					"hello": "world",
				},
			},
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedString: `[{"foo":"bar"},{"hello":"world"}]`,
		},
		{
			name:   "string",
			colVal: "hello world",
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedString: "hello world",
		},
	}

	for _, testCase := range testCases {
		evaluateTestCase(r.T(), r.store, testCase)
	}
}

// TestCastColValStaging_Time - will test all the variants of date, time and date time.
func (r *RedshiftTestSuite) TestCastColValStaging_Time() {
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
	assert.NoError(r.T(), err)

	birthTime, err := ext.NewExtendedTime(birthday, timeKind.ExtendedTimeDetails.Type, "")
	assert.NoError(r.T(), err)

	birthDateTime, err := ext.NewExtendedTime(birthday, dateTimeKind.ExtendedTimeDetails.Type, "")
	assert.NoError(r.T(), err)

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
			name:   "date (but value is datetime)",
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
		evaluateTestCase(r.T(), r.store, testCase)
	}
}

func (r *RedshiftTestSuite) TestCastColValStaging_TOAST() {
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
		evaluateTestCase(r.T(), r.store, testCase)
	}
}

func (r *RedshiftTestSuite) TestCastColValStaging_ExceededValues() {
	testCases := []_testCase{
		{
			name:   "string",
			colVal: stringutil.Random(maxRedshiftVarCharLen + 1),
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedString: "__artie_exceeded_value",
		},
		{
			name:   "string",
			colVal: "thisissuperlongbutnotlongenoughtogetmasked",
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedString: "thisissuperlongbutnotlongenoughtogetmasked",
		},
		{
			name:   "struct",
			colVal: map[string]interface{}{"foo": stringutil.Random(maxRedshiftSuperLen + 1)},
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedString: `{"key":"__artie_exceeded_value"}`,
		},
		{
			name:   "struct",
			colVal: map[string]interface{}{"foo": stringutil.Random(maxRedshiftSuperLen + 1)},
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedString: `{"key":"__artie_exceeded_value"}`,
		},
	}

	ctx := config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
		Config: &config.Config{
			Redshift: &config.Redshift{
				SkipLgCols: true,
			},
		},
	})

	store := db.Store(r.fakeStore)
	skipLargeRowsStore := LoadRedshift(ctx, &store)

	for _, testCase := range testCases {
		evaluateTestCase(r.T(), skipLargeRowsStore, testCase)
	}

}
