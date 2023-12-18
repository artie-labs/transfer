package typing

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
)

func (t *TypingTestSuite) Test_KindToDWHType() {
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
		t.Equal(tc.expectedSnowflakeType, KindToDWHType(tc.kd, constants.Snowflake), idx)
		t.Equal(tc.expectedBigQueryType, KindToDWHType(tc.kd, constants.BigQuery), idx)
		t.Equal(tc.expectedRedshiftType, KindToDWHType(tc.kd, constants.Redshift), idx)
	}
}
