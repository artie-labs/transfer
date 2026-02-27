package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
)

func TestMSSQLDialect_DataTypeForKind(t *testing.T) {
	tcs := []struct {
		kd typing.KindDetails
		// MSSQL is sensitive based on primary key
		expected     string
		expectedIsPk string
	}{
		{
			kd:           typing.String,
			expected:     "VARCHAR(MAX)",
			expectedIsPk: "VARCHAR(900)",
		},
		{
			kd: typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: typing.ToPtr(int32(12345)),
			},
			expected:     "VARCHAR(12345)",
			expectedIsPk: "VARCHAR(900)",
		},
	}

	for idx, tc := range tcs {
		actual, err := MSSQLDialect{}.DataTypeForKind(tc.kd, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, actual, idx)

		actual, err = MSSQLDialect{}.DataTypeForKind(tc.kd, true, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedIsPk, actual, idx)
	}
	{
		// Interval
		actual, err := MSSQLDialect{}.DataTypeForKind(typing.Interval, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "NVARCHAR(MAX)", actual)
	}
}

func TestMSSQLDialect_KindForDataType(t *testing.T) {
	dialect := MSSQLDialect{}
	{
		// Invalid
		_, err := dialect.KindForDataType("invalid")
		assert.True(t, typing.IsUnsupportedDataTypeError(err))
	}

	colToExpectedKind := map[string]typing.KindDetails{
		"smallint":       typing.Integer,
		"tinyint":        typing.Integer,
		"int":            typing.Integer,
		"float":          typing.Float,
		"real":           typing.Float,
		"bit":            typing.Boolean,
		"date":           typing.Date,
		"time":           typing.TimeKindDetails,
		"datetime":       typing.TimestampNTZ,
		"datetime2":      typing.TimestampNTZ,
		"datetimeoffset": typing.TimestampTZ,
	}

	for col, expectedKind := range colToExpectedKind {
		kd, err := dialect.KindForDataType(col)
		assert.NoError(t, err)
		assert.Equal(t, expectedKind.Kind, kd.Kind, col)
	}
	{
		// String types, that are all precision 55.
		stringTypeMap := map[string]typing.KindDetails{
			"char(55)":     typing.String,
			"varchar(55)":  typing.String,
			"nchar(55)":    typing.String,
			"nvarchar(55)": typing.String,
			"ntext(55)":    typing.String,
			"text(55)":     typing.String,
		}

		for rawType, expectedKind := range stringTypeMap {
			kd, err := dialect.KindForDataType(rawType)
			assert.NoError(t, err)
			assert.Equal(t, expectedKind.Kind, kd.Kind, rawType)
			assert.Equal(t, int32(55), *kd.OptionalStringPrecision, rawType)
		}
	}

	{
		_, err := dialect.KindForDataType("numeric(5")
		assert.ErrorContains(t, err, "missing closing parenthesis")
	}
	{
		kd, err := dialect.KindForDataType("numeric(5, 2)")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
	}
}
