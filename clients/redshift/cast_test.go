package redshift

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/db"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"

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
			colVal: stringutil.Random(maxRedshiftLength + 1),
			colKind: columns.Column{
				KindDetails: typing.String,
			},
			expectedResult: constants.ExceededValueMarker,
		},
		{
			name:   "string (specified string precision)",
			colVal: "hello dusty",
			colKind: columns.Column{
				KindDetails: typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: ptr.ToInt(3),
				},
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
			colVal: fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(maxRedshiftLength+1)),
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedResult: fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker),
		},
		{
			name:   "string, but the data type is a SUPER",
			colVal: stringutil.Random(maxRedshiftLength + 1),
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
	colVal  any
	colKind columns.Column

	expectedString string
	errorMessage   string
}

func evaluateTestCase(t *testing.T, store *Store, testCase _testCase) {
	actualString, actualErr := store.CastColValStaging(testCase.colVal, testCase.colKind, nil)
	if len(testCase.errorMessage) > 0 {
		assert.ErrorContains(t, actualErr, testCase.errorMessage, testCase.name)
	} else {
		assert.NoError(t, actualErr, testCase.name)
		assert.Equal(t, testCase.expectedString, actualString, testCase.name)
	}
}

func (r *RedshiftTestSuite) TestCastColValStaging_ExceededValues() {
	testCases := []_testCase{
		{
			name:   "string",
			colVal: stringutil.Random(maxRedshiftLength + 1),
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
			colVal: map[string]any{"foo": stringutil.Random(maxRedshiftLength + 1)},
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedString: `{"key":"__artie_exceeded_value"}`,
		},
		{
			name:   "struct",
			colVal: map[string]any{"foo": stringutil.Random(maxRedshiftLength + 1)},
			colKind: columns.Column{
				KindDetails: typing.Struct,
			},
			expectedString: `{"key":"__artie_exceeded_value"}`,
		},
	}

	cfg := config.Config{
		Redshift: &config.Redshift{},
	}

	store := db.Store(r.fakeStore)
	skipLargeRowsStore, err := LoadRedshift(cfg, &store)
	assert.NoError(r.T(), err)

	for _, testCase := range testCases {
		evaluateTestCase(r.T(), skipLargeRowsStore, testCase)
	}

}
