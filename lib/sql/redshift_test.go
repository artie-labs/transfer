package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
)

func TestRedshiftDialect_KindForDataType(t *testing.T) {
	type rawTypeAndPrecision struct {
		rawType   string
		precision string
	}

	type _testCase struct {
		name       string
		rawTypes   []rawTypeAndPrecision
		expectedKd typing.KindDetails
	}

	testCases := []_testCase{
		{
			name: "Integer",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "integer"},
				{rawType: "bigint"},
				{rawType: "INTEGER"},
			},
			expectedKd: typing.Integer,
		},
		{
			name: "String w/o precision",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "character varying"},
				{rawType: "character varying(65535)"},
				{
					rawType:   "character varying",
					precision: "not a number",
				},
			},
			expectedKd: typing.String,
		},
		{
			name: "String w/ precision",
			rawTypes: []rawTypeAndPrecision{
				{
					rawType:   "character varying",
					precision: "65535",
				},
			},
			expectedKd: typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: ptr.ToInt(65535),
			},
		},
		{
			name: "Double Precision",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "double precision"},
				{rawType: "DOUBLE precision"},
			},
			expectedKd: typing.Float,
		},
		{
			name: "Time",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "timestamp with time zone"},
				{rawType: "timestamp without time zone"},
				{rawType: "time without time zone"},
				{rawType: "date"},
			},
			expectedKd: typing.ETime,
		},
		{
			name: "Boolean",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "boolean"},
			},
			expectedKd: typing.Boolean,
		},
		{
			name: "numeric",
			rawTypes: []rawTypeAndPrecision{
				{rawType: "numeric(5,2)"},
				{rawType: "numeric(5,5)"},
			},
			expectedKd: typing.EDecimal,
		},
	}

	for _, testCase := range testCases {
		for _, rawTypeAndPrec := range testCase.rawTypes {
			kd, err := RedshiftDialect{}.KindForDataType(rawTypeAndPrec.rawType, rawTypeAndPrec.precision)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedKd.Kind, kd.Kind, testCase.name)

			if kd.OptionalStringPrecision != nil {
				assert.Equal(t, *testCase.expectedKd.OptionalStringPrecision, *kd.OptionalStringPrecision, testCase.name)
			} else {
				assert.Nil(t, kd.OptionalStringPrecision, testCase.name)
			}
		}
	}
}
