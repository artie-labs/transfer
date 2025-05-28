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
		assert.Equal(t, tc.expected, MSSQLDialect{}.DataTypeForKind(tc.kd, false, config.SharedDestinationColumnSettings{}), idx)
		assert.Equal(t, tc.expectedIsPk, MSSQLDialect{}.DataTypeForKind(tc.kd, true, config.SharedDestinationColumnSettings{}), idx)
	}
}

func TestMSSQLDialect_KindForDataType(t *testing.T) {
	dialect := MSSQLDialect{}

	colToExpectedKind := map[string]typing.KindDetails{
		"char":           typing.String,
		"varchar":        typing.String,
		"nchar":          typing.String,
		"nvarchar":       typing.String,
		"ntext":          typing.String,
		"text":           typing.String,
		"smallint":       typing.Integer,
		"tinyint":        typing.Integer,
		"int":            typing.Integer,
		"float":          typing.Float,
		"real":           typing.Float,
		"bit":            typing.Boolean,
		"date":           typing.Date,
		"time":           typing.Time,
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
	{
		kd, err := dialect.KindForDataType("char(5)")
		assert.NoError(t, err)
		assert.Equal(t, typing.String.Kind, kd.Kind)
		assert.Equal(t, int32(5), *kd.OptionalStringPrecision)
	}
}
