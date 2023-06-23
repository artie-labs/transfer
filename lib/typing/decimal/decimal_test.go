package decimal

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/ptr"
)

func TestDecimalKind(t *testing.T) {
	type _testCase struct {
		Name      string
		Precision int
		Scale     int

		ExpectedSnowflakeKind string
		ExpectedRedshiftKind  string
		ExpectedBigQueryKind  string
	}

	testCases := []_testCase{
		{
			Name:                  "-1 precision",
			Precision:             -1,
			ExpectedSnowflakeKind: "STRING",
			ExpectedRedshiftKind:  "TEXT",
			ExpectedBigQueryKind:  "STRING",
		},
		{
			Name:                  "numeric(39, 5)",
			Precision:             39,
			Scale:                 5,
			ExpectedSnowflakeKind: "STRING",
			ExpectedRedshiftKind:  "TEXT",
			ExpectedBigQueryKind:  "STRING",
		},
		{
			Name:                  "numeric(38, 2)",
			Precision:             38,
			Scale:                 2,
			ExpectedSnowflakeKind: "NUMERIC(38, 2)",
			ExpectedRedshiftKind:  "NUMERIC(38, 2)",
			ExpectedBigQueryKind:  "STRING",
		},
		{
			Name:                  "numeric(31, 2)",
			Precision:             31,
			Scale:                 2,
			ExpectedSnowflakeKind: "NUMERIC(31, 2)",
			ExpectedRedshiftKind:  "NUMERIC(31, 2)",
			ExpectedBigQueryKind:  "NUMERIC(31, 2)",
		},
	}

	for _, testCase := range testCases {
		d := NewDecimal(testCase.Scale, ptr.ToInt(testCase.Precision), nil)
		assert.Equal(t, testCase.ExpectedSnowflakeKind, d.SnowflakeKind(), testCase.Name)
		assert.Equal(t, testCase.ExpectedRedshiftKind, d.RedshiftKind(), testCase.Name)
		assert.Equal(t, testCase.ExpectedBigQueryKind, d.BigQueryKind(), testCase.Name)
	}
}
