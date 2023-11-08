package typing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedshiftTypeToKind(t *testing.T) {
	type _testCase struct {
		name       string
		rawTypes   []string
		expectedKd KindDetails
	}

	testCases := []_testCase{
		{
			name:       "Integer",
			rawTypes:   []string{"integer", "bigint", "INTEGER"},
			expectedKd: Integer,
		},
		{
			name:       "String",
			rawTypes:   []string{"character varying"},
			expectedKd: String,
		},
		{
			name:       "String",
			rawTypes:   []string{"character varying"},
			expectedKd: String,
		},
		{
			name:       "String",
			rawTypes:   []string{"character varying(65535)"},
			expectedKd: String,
		},
		{
			name:       "Double Precision",
			rawTypes:   []string{"double precision", "DOUBLE precision"},
			expectedKd: Float,
		},
		{
			name:       "Time",
			rawTypes:   []string{"timestamp with time zone", "timestamp without time zone", "time without time zone", "date"},
			expectedKd: ETime,
		},
		{
			name:       "Boolean",
			rawTypes:   []string{"boolean"},
			expectedKd: Boolean,
		},
		{
			name:       "numeric",
			rawTypes:   []string{"numeric(5,2)", "numeric(5,5)"},
			expectedKd: EDecimal,
		},
	}

	for _, testCase := range testCases {
		for _, rawType := range testCase.rawTypes {
			kd := RedshiftTypeToKind(rawType)
			assert.Equal(t, testCase.expectedKd.Kind, kd.Kind, testCase.name)
		}
	}
}
