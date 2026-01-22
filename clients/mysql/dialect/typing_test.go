package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
)

func TestMySQLDialect_DataTypeForKind(t *testing.T) {
	tcs := []struct {
		kd           typing.KindDetails
		expected     string
		expectedIsPk string
	}{
		{
			kd:           typing.String,
			expected:     "TEXT",
			expectedIsPk: "VARCHAR(255)",
		},
		{
			kd: typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: typing.ToPtr(int32(12345)),
			},
			expected:     "VARCHAR(12345)",
			expectedIsPk: "VARCHAR(255)",
		},
		{
			kd: typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: typing.ToPtr(int32(100)),
			},
			expected:     "VARCHAR(100)",
			expectedIsPk: "VARCHAR(100)",
		},
		{
			kd:           typing.Boolean,
			expected:     "BOOLEAN",
			expectedIsPk: "BOOLEAN",
		},
		{
			kd:           typing.Integer,
			expected:     "BIGINT",
			expectedIsPk: "BIGINT",
		},
		{
			kd:           typing.Float,
			expected:     "DOUBLE",
			expectedIsPk: "DOUBLE",
		},
		{
			kd:           typing.Date,
			expected:     "DATE",
			expectedIsPk: "DATE",
		},
		{
			kd:           typing.TimeKindDetails,
			expected:     "TIME(6)",
			expectedIsPk: "TIME(6)",
		},
		{
			kd:           typing.TimestampNTZ,
			expected:     "DATETIME(6)",
			expectedIsPk: "DATETIME(6)",
		},
		{
			kd:           typing.TimestampTZ,
			expected:     "DATETIME(6)",
			expectedIsPk: "DATETIME(6)",
		},
		{
			kd:           typing.Struct,
			expected:     "JSON",
			expectedIsPk: "JSON",
		},
		{
			kd:           typing.Array,
			expected:     "JSON",
			expectedIsPk: "JSON",
		},
	}

	for _, tc := range tcs {
		actual, err := MySQLDialect{}.DataTypeForKind(tc.kd, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, actual)

		actual, err = MySQLDialect{}.DataTypeForKind(tc.kd, true, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedIsPk, actual)
	}
}

func TestMySQLDialect_KindForDataType(t *testing.T) {
	dialect := MySQLDialect{}

	// Invalid type
	{
		_, err := dialect.KindForDataType("invalid")
		assert.True(t, typing.IsUnsupportedDataTypeError(err))
	}

	// Simple type mappings
	colToExpectedKind := map[string]typing.KindDetails{
		// Integer types
		"tinyint":   typing.Integer,
		"smallint":  typing.Integer,
		"mediumint": typing.Integer,
		"int":       typing.Integer,
		"integer":   typing.Integer,
		"bigint":    typing.Integer,
		// Float types
		"float":  typing.Float,
		"double": typing.Float,
		"real":   typing.Float,
		// Boolean types - MySQL stores BOOLEAN as tinyint(1), but when we read
		// from INFORMATION_SCHEMA, the DDL query now returns "boolean" for tinyint(1)
		"boolean": typing.Boolean,
		"bool":    typing.Boolean,
		"bit":     typing.Boolean,
		// Date/Time types
		"date":      typing.Date,
		"time":      typing.TimeKindDetails,
		"datetime":  typing.TimestampNTZ,
		"timestamp": typing.TimestampNTZ,
		// JSON type
		"json": typing.Struct,
		// Text types without precision
		"text":       typing.String,
		"tinytext":   typing.String,
		"mediumtext": typing.String,
		"longtext":   typing.String,
		// Binary types
		"binary":     typing.String,
		"varbinary":  typing.String,
		"tinyblob":   typing.String,
		"blob":       typing.String,
		"mediumblob": typing.String,
		"longblob":   typing.String,
	}

	for col, expectedKind := range colToExpectedKind {
		kd, err := dialect.KindForDataType(col)
		assert.NoError(t, err)
		assert.Equal(t, expectedKind.Kind, kd.Kind, col)
	}

	// String types with precision
	{
		stringTypeMap := map[string]int32{
			"char(55)":    55,
			"varchar(55)": 55,
			"char(255)":   255,
			"varchar(50)": 50,
		}

		for rawType, expectedPrecision := range stringTypeMap {
			kd, err := dialect.KindForDataType(rawType)
			assert.NoError(t, err)
			assert.Equal(t, typing.String.Kind, kd.Kind, rawType)
			assert.Equal(t, expectedPrecision, *kd.OptionalStringPrecision, rawType)
		}
	}

	// Decimal types
	{
		_, err := dialect.KindForDataType("decimal(5")
		assert.ErrorContains(t, err, "missing closing parenthesis")
	}
	{
		kd, err := dialect.KindForDataType("decimal(10, 2)")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, int32(10), kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
	}
	{
		kd, err := dialect.KindForDataType("numeric(15, 5)")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, int32(15), kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Scale())
	}
}
