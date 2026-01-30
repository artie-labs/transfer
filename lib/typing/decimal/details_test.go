package decimal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetails_BigQueryKind(t *testing.T) {
	// Variable numeric
	details := NewDetails(PrecisionNotSpecified, DefaultScale)
	{
		// numericTypeForVariableNumeric = false
		assert.Equal(t, "NUMERIC", details.BigQueryKind(false))
	}
	{
		// numericTypeForVariableNumeric = true
		assert.Equal(t, "BIGNUMERIC", details.BigQueryKind(true))
	}
}

func TestDetails_NotSet(t *testing.T) {
	details := NewDetails(PrecisionNotSpecified, DefaultScale)
	assert.True(t, details.NotSet())
}

func TestDecimalDetailsKind(t *testing.T) {
	type _testCase struct {
		Name      string
		Precision int32
		Scale     int32

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
			ExpectedBigQueryKind:  "NUMERIC",
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
		d := NewDetails(testCase.Precision, testCase.Scale)
		assert.Equal(t, testCase.ExpectedSnowflakeKind, d.SnowflakeKind(), testCase.Name)
		assert.Equal(t, testCase.ExpectedRedshiftKind, d.RedshiftKind(), testCase.Name)
		assert.Equal(t, testCase.ExpectedBigQueryKind, d.BigQueryKind(false), testCase.Name)
		assert.False(t, d.NotSet(), testCase.Name)
	}
}

func TestDetails_PostgresKind(t *testing.T) {
	detailsToValueMap := map[Details]string{
		NewDetails(PrecisionNotSpecified, DefaultScale): "NUMERIC",
		NewDetails(10, 2):  "NUMERIC(10, 2)",
		NewDetails(10, 0):  "NUMERIC(10, 0)",
		NewDetails(47, 20): "NUMERIC(47, 20)",
	}

	for details, expectedValue := range detailsToValueMap {
		assert.Equal(t, expectedValue, details.PostgresKind())
	}
}

func TestDetails_DuckDBKind(t *testing.T) {
	testCases := []struct {
		name     string
		details  Details
		expected string
	}{
		{
			name:     "variable precision",
			details:  NewDetails(PrecisionNotSpecified, DefaultScale),
			expected: "DECIMAL",
		},
		{
			name:     "precision within limit",
			details:  NewDetails(10, 2),
			expected: "DECIMAL(10, 2)",
		},
		{
			name:     "precision at max limit (38)",
			details:  NewDetails(38, 2),
			expected: "DECIMAL(38, 2)",
		},
		{
			name:     "precision exceeds limit (39)",
			details:  NewDetails(39, 5),
			expected: "TEXT",
		},
		{
			name:     "precision exceeds limit (76)",
			details:  NewDetails(76, 38),
			expected: "TEXT",
		},
		{
			name:     "zero scale",
			details:  NewDetails(15, 0),
			expected: "DECIMAL(15, 0)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.details.DuckDBKind())
		})
	}
}

func TestDetails_MySQLKind(t *testing.T) {
	expectedMapToDetails := map[Details]string{
		NewDetails(PrecisionNotSpecified, DefaultScale): "DECIMAL(65, 5)",
		NewDetails(10, 2):  "DECIMAL(10, 2)",
		NewDetails(65, 30): "DECIMAL(65, 30)",
		NewDetails(66, 5):  "TEXT",
		NewDetails(40, 31): "TEXT",
		NewDetails(15, 0):  "DECIMAL(15, 0)",
	}

	for details, expectedValue := range expectedMapToDetails {
		assert.Equal(t, expectedValue, details.MySQLKind())
	}
}
