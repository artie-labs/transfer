package typing

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
	}

	for idx, tc := range tcs {
		t.Equal(tc.expectedSnowflakeType, kindToSnowflake(tc.kd), idx)
		t.Equal(tc.expectedBigQueryType, kindToBigQuery(tc.kd), idx)
		t.Equal(tc.expectedRedshiftType, kindToRedShift(tc.kd), idx)
	}
}
