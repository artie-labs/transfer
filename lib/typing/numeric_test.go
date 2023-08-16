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
		name              string
		input             string
		expectedPrecision int
		expectedScale     int
	}

	testCases := []_testCase{
		{
			name:              "precision and scale",
			input:             "numeric(5, 2)",
			expectedPrecision: 5,
			expectedScale:     2,
		},
	}

	for _, tc := range testCases {
		numeric := ParseNumeric(defaultPrefix, tc.input)
		assert.Equal(t, tc.expectedPrecision, *numeric.ExtendedDecimalDetails.Precision(), tc.name)
		assert.Equal(t, tc.expectedScale, numeric.ExtendedDecimalDetails.Scale(), tc.name)
	}
}
