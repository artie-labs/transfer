package typing

import (
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/stretchr/testify/assert"
)

func TestParseNumeric(t *testing.T) {
	type _testCase struct {
		valString           string
		expectedKindDetails KindDetails
		expectedPrecision   *int // Using a pointer to int so we can differentiate between unset (nil) and set (0 included)
		expectedScale       int
	}

	testCases := []_testCase{
		{
			valString:           "numeri232321c(5,2)",
			expectedKindDetails: Invalid,
		},
		{
			valString:           "numeric",
			expectedKindDetails: Invalid,
		},
		{
			valString:           "numeric(5, a)",
			expectedKindDetails: Invalid,
		},
		{
			valString:           "numeric(b, 5)",
			expectedKindDetails: Invalid,
		},
		{
			valString:           "numeric(b, a)",
			expectedKindDetails: Invalid,
		},
		{
			valString:           "numeric(5, 2)",
			expectedKindDetails: EDecimal,
			expectedPrecision:   ptr.ToInt(5),
			expectedScale:       2,
		},
		{
			valString:           "numeric(5,2)",
			expectedKindDetails: EDecimal,
			expectedPrecision:   ptr.ToInt(5),
			expectedScale:       2,
		},
		{
			valString:           "numeric(39, 6)",
			expectedKindDetails: EDecimal,
			expectedPrecision:   ptr.ToInt(39),
			expectedScale:       6,
		},
		{
			valString:           "numeric(5)",
			expectedKindDetails: Integer,
			expectedPrecision:   ptr.ToInt(5),
			expectedScale:       0,
		},
		{
			valString:           "numeric(5, 0)",
			expectedKindDetails: Integer,
			expectedPrecision:   ptr.ToInt(5),
			expectedScale:       0,
		},
	}

	for _, testCase := range testCases {
		result := ParseNumeric(defaultPrefix, testCase.valString)
		assert.Equal(t, testCase.expectedKindDetails.Kind, result.Kind, testCase.valString)
		if result.ExtendedDecimalDetails != nil {
			assert.Equal(t, testCase.expectedScale, result.ExtendedDecimalDetails.Scale(), testCase.valString)

			if result.ExtendedDecimalDetails.Precision() != nil {
				assert.Equal(t, *testCase.expectedPrecision, *result.ExtendedDecimalDetails.Precision(), testCase.valString)
			}
		}
	}
}
