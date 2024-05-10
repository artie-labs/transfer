package typing

import (
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/stretchr/testify/assert"
)

func TestParseNumeric(t *testing.T) {
	type _testCase struct {
		parameters          []string
		expectedKindDetails KindDetails
		expectedPrecision   *int // Using a pointer to int so we can differentiate between unset (nil) and set (0 included)
		expectedScale       int
	}

	testCases := []_testCase{
		{
			parameters:          []string{},
			expectedKindDetails: Invalid,
		},
		{
			parameters:          []string{"5", "a"},
			expectedKindDetails: Invalid,
		},
		{
			parameters:          []string{"b", "5"},
			expectedKindDetails: Invalid,
		},
		{
			parameters:          []string{"a", "b"},
			expectedKindDetails: Invalid,
		},
		{
			parameters:          []string{"1", "2", "3"},
			expectedKindDetails: Invalid,
		},
		{
			parameters:          []string{"5", " 2"},
			expectedKindDetails: EDecimal,
			expectedPrecision:   ptr.ToInt(5),
			expectedScale:       2,
		},
		{
			parameters:          []string{"5", "2"},
			expectedKindDetails: EDecimal,
			expectedPrecision:   ptr.ToInt(5),
			expectedScale:       2,
		},
		{
			parameters:          []string{"39", "6"},
			expectedKindDetails: EDecimal,
			expectedPrecision:   ptr.ToInt(39),
			expectedScale:       6,
		},
		{
			parameters:          []string{"5"},
			expectedKindDetails: Integer,
			expectedPrecision:   ptr.ToInt(5),
			expectedScale:       0,
		},
		{
			parameters:          []string{"5", "0"},
			expectedKindDetails: Integer,
			expectedPrecision:   ptr.ToInt(5),
			expectedScale:       0,
		},
	}

	for _, testCase := range testCases {
		result := ParseNumeric(testCase.parameters)
		assert.Equal(t, testCase.expectedKindDetails.Kind, result.Kind, testCase.parameters)
		if result.ExtendedDecimalDetails != nil {
			assert.Equal(t, testCase.expectedScale, result.ExtendedDecimalDetails.Scale(), testCase.parameters)

			if result.ExtendedDecimalDetails.Precision() != nil {
				assert.Equal(t, *testCase.expectedPrecision, *result.ExtendedDecimalDetails.Precision(), testCase.parameters)
			}
		}
	}
}
