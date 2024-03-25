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
			Name:                  "numeric(39, 0)",
			Precision:             39,
			Scale:                 0,
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
			ExpectedBigQueryKind:  "BIGNUMERIC(39, 5)",
		},
		{
			Name:                  "numeric(38, 2)",
			Precision:             38,
			Scale:                 2,
			ExpectedSnowflakeKind: "NUMERIC(38, 2)",
			ExpectedRedshiftKind:  "NUMERIC(38, 2)",
			ExpectedBigQueryKind:  "BIGNUMERIC(38, 2)",
		},
		{
			Name:                  "numeric(31, 2)",
			Precision:             31,
			Scale:                 2,
			ExpectedSnowflakeKind: "NUMERIC(31, 2)",
			ExpectedRedshiftKind:  "NUMERIC(31, 2)",
			ExpectedBigQueryKind:  "NUMERIC(31, 2)",
		},
		{
			Name:                  "bignumeric(76, 38)",
			Precision:             76,
			Scale:                 38,
			ExpectedSnowflakeKind: "STRING",
			ExpectedRedshiftKind:  "TEXT",
			ExpectedBigQueryKind:  "BIGNUMERIC(76, 38)",
		},
	}

	for _, testCase := range testCases {
		d := NewDecimal(ptr.ToInt(testCase.Precision), testCase.Scale, nil)
		assert.Equal(t, testCase.ExpectedSnowflakeKind, d.SnowflakeKind(), testCase.Name)
		assert.Equal(t, testCase.ExpectedRedshiftKind, d.RedshiftKind(), testCase.Name)
		assert.Equal(t, testCase.ExpectedBigQueryKind, d.BigQueryKind(), testCase.Name)
	}
}
