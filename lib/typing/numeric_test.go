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
		assert.Equal(t, testCase.expectedKindDetails, ParseNumeric(testCase.prefix, testCase.valString), fmt.Sprintf("prefix:%s, valString:%s", testCase.prefix, testCase.valString))
	}
}
