package decimal

import (
	"testing"

	"github.com/cockroachdb/apd/v3"
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

func mustParseDecimal(value string) *apd.Decimal {
	decimal, _, err := apd.NewFromString(value)
	if err != nil {
		panic(err)
	}
	return decimal
}

func TestDecimalWithNewExponent(t *testing.T) {
	assert.Equal(t, "0", DecimalWithNewExponent(apd.New(0, 0), 0).Text('f'))
	assert.Equal(t, "00", DecimalWithNewExponent(apd.New(0, 1), 1).Text('f'))
	assert.Equal(t, "0", DecimalWithNewExponent(apd.New(0, 100), 0).Text('f'))
	assert.Equal(t, "00", DecimalWithNewExponent(apd.New(0, 0), 1).Text('f'))
	assert.Equal(t, "0.0", DecimalWithNewExponent(apd.New(0, 0), -1).Text('f'))

	// Same exponent:
	assert.Equal(t, "12.349", DecimalWithNewExponent(mustParseDecimal("12.349"), -3).Text('f'))
	// More precise exponent:
	assert.Equal(t, "12.3490", DecimalWithNewExponent(mustParseDecimal("12.349"), -4).Text('f'))
	assert.Equal(t, "12.34900", DecimalWithNewExponent(mustParseDecimal("12.349"), -5).Text('f'))
	// Lest precise exponent:
	// Extra digits should be truncated rather than rounded.
	assert.Equal(t, "12.34", DecimalWithNewExponent(mustParseDecimal("12.349"), -2).Text('f'))
	assert.Equal(t, "12.3", DecimalWithNewExponent(mustParseDecimal("12.349"), -1).Text('f'))
	assert.Equal(t, "12", DecimalWithNewExponent(mustParseDecimal("12.349"), 0).Text('f'))
	assert.Equal(t, "10", DecimalWithNewExponent(mustParseDecimal("12.349"), 1).Text('f'))
}
