package typing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseNumeric(t *testing.T) {
	type _testCase struct {
		prefix              string
		valString           string
		expectedKindDetails KindDetails
	}

	testCases := []_testCase{
		{
			prefix:              "random prefix",
			valString:           "numeri232321c(5,2)",
			expectedKindDetails: Invalid,
		},
		{
			prefix:              defaultPrefix,
			valString:           "numeric",
			expectedKindDetails: Invalid,
		},
		{
			prefix:              defaultPrefix,
			valString:           "numeric(5, a)",
			expectedKindDetails: Invalid,
		},
		{
			prefix:              defaultPrefix,
			valString:           "numeric(b, 5)",
			expectedKindDetails: Invalid,
		},
		{
			prefix:              defaultPrefix,
			valString:           "numeric(b, a)",
			expectedKindDetails: Invalid,
		},
		{
			prefix:              defaultPrefix,
			valString:           "numeric(5, 2)",
			expectedKindDetails: EDecimal,
		},
		{
			prefix:              defaultPrefix,
			valString:           "numeric(5,2)",
			expectedKindDetails: EDecimal,
		},
		{
			prefix:              defaultPrefix,
			valString:           "numeric(5)",
			expectedKindDetails: Integer,
		},
		{
			prefix:              defaultPrefix,
			valString:           "numeric(5, 0)",
			expectedKindDetails: Integer,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expectedKindDetails.Kind, ParseNumeric(testCase.prefix, testCase.valString).Kind, fmt.Sprintf("prefix:%s, valString:%s", testCase.prefix, testCase.valString))
	}
}

func TestParseNumeric_PrecisionAndScale(t *testing.T) {
	type _testCase struct {
		inputs            []string
		expectedPrecision int
		expectedScale     int
	}

	testCases := []_testCase{
		{
			inputs: []string{
				"numeric(5, 2)",
				"numeric(5,2)",
			},
			expectedPrecision: 5,
			expectedScale:     2,
		},
		{
			inputs: []string{
				"numeric(39, 4)",
				"numeric(39,4)",
			},
			expectedPrecision: 39,
			expectedScale:     4,
		},
	}

	for _, tc := range testCases {
		for _, input := range tc.inputs {
			numeric := ParseNumeric(defaultPrefix, input)
			assert.Equal(t, tc.expectedPrecision, *numeric.ExtendedDecimalDetails.Precision(), input)
			assert.Equal(t, tc.expectedScale, numeric.ExtendedDecimalDetails.Scale(), input)
		}

	}
}
