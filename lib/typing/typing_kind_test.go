package typing

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

func Test_KindToDWHType(t *testing.T) {
	type _tc struct {
		kd                    KindDetails
		expectedSnowflakeType string
		expectedBigQueryType  string
		expectedRedshiftType  string

		// MSSQL is sensitive based on primary key
		expectedMSSQLType   string
		expectedMSSQLTypePk string
	}

	tcs := []_tc{
		{
			kd:                    String,
			expectedSnowflakeType: "string",
			expectedBigQueryType:  "string",
			expectedRedshiftType:  "VARCHAR(MAX)",
			expectedMSSQLType:     "VARCHAR(MAX)",
			expectedMSSQLTypePk:   "VARCHAR(900)",
		},
		{
			kd: KindDetails{
				Kind:                    String.Kind,
				OptionalStringPrecision: ptr.ToInt(12345),
			},
			expectedSnowflakeType: "string",
			expectedBigQueryType:  "string",
			expectedRedshiftType:  "VARCHAR(12345)",
			expectedMSSQLType:     "VARCHAR(12345)",
			expectedMSSQLTypePk:   "VARCHAR(900)",
		},
	}

	for idx, tc := range tcs {
		for _, isPk := range []bool{true, false} {
			assert.Equal(t, tc.expectedSnowflakeType, KindToDWHType(tc.kd, constants.Snowflake, isPk), idx)
			assert.Equal(t, tc.expectedBigQueryType, KindToDWHType(tc.kd, constants.BigQuery, isPk), idx)
			assert.Equal(t, tc.expectedRedshiftType, KindToDWHType(tc.kd, constants.Redshift, isPk), idx)
		}

		assert.Equal(t, tc.expectedMSSQLType, KindToDWHType(tc.kd, constants.MSSQL, false), idx)
		assert.Equal(t, tc.expectedMSSQLTypePk, KindToDWHType(tc.kd, constants.MSSQL, true), idx)
	}
}
