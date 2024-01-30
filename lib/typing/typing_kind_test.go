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
	}

	tcs := []_tc{
		{
			kd:                    String,
			expectedSnowflakeType: "string",
			expectedBigQueryType:  "string",
			expectedRedshiftType:  "VARCHAR(MAX)",
		},
		{
			kd: KindDetails{
				Kind:                         String.Kind,
				OptionalRedshiftStrPrecision: ptr.ToInt(12345),
			},
			expectedSnowflakeType: "string",
			expectedBigQueryType:  "string",
			expectedRedshiftType:  "VARCHAR(12345)",
		},
	}

	for idx, tc := range tcs {
		assert.Equal(t, tc.expectedSnowflakeType, KindToDWHType(tc.kd, constants.Snowflake), idx)
		assert.Equal(t, tc.expectedBigQueryType, KindToDWHType(tc.kd, constants.BigQuery), idx)
		assert.Equal(t, tc.expectedRedshiftType, KindToDWHType(tc.kd, constants.Redshift), idx)
	}
}
