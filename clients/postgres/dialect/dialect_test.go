package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestKindForDataType(t *testing.T) {
	expectedTypeToKindMap := map[string]typing.KindDetails{
		// Numbers:
		"numeric(5, 2)": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 2)),
		"numeric(5, 0)": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)),
		"numeric(5)":    typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)),
		// Variable numeric type:
		"numeric":          typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)),
		"float":            typing.Float,
		"real":             typing.Float,
		"double":           typing.Float,
		"double precision": typing.Float,
		"smallint":         typing.BuildIntegerKind(typing.SmallIntegerKind),
		"integer":          typing.BuildIntegerKind(typing.IntegerKind),
		"bigint":           typing.BuildIntegerKind(typing.BigIntegerKind),
		// String data types:
		"character varying(5)": {Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(5))},
		"character(5)":         {Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(5))},
		"text":                 typing.String,
		// Boolean:
		"boolean": typing.Boolean,
		// Date and timestamp data types:
		"date":                           typing.Date,
		"time":                           typing.TimeKindDetails,
		"timestamp with time zone":       typing.TimestampTZ,
		"timestamp(5) with time zone":    typing.TimestampTZ,
		"timestamp without time zone":    typing.TimestampNTZ,
		"timestamp(4) without time zone": typing.TimestampNTZ,
		// Other data types:
		"json": typing.Struct,
	}

	for dataType, expectedKind := range expectedTypeToKindMap {
		kind, err := PostgresDialect{}.KindForDataType(dataType)
		assert.NoError(t, err, dataType)
		assert.Equal(t, expectedKind, kind)
	}
}

func TestKindForDataType_Arrays(t *testing.T) {
	intKind := typing.BuildIntegerKind(typing.IntegerKind)
	bigintKind := typing.BuildIntegerKind(typing.BigIntegerKind)

	tests := []struct {
		dataType string
		expected typing.KindDetails
	}{
		{
			dataType: "integer[]",
			expected: typing.KindDetails{Kind: typing.Array.Kind, OptionalArrayKind: &intKind},
		},
		{
			dataType: "bigint[]",
			expected: typing.KindDetails{Kind: typing.Array.Kind, OptionalArrayKind: &bigintKind},
		},
		{
			dataType: "boolean[]",
			expected: typing.KindDetails{Kind: typing.Array.Kind, OptionalArrayKind: &typing.Boolean},
		},
		{
			dataType: "text[]",
			expected: typing.KindDetails{Kind: typing.Array.Kind, OptionalArrayKind: &typing.String},
		},
		{
			dataType: "double precision[]",
			expected: typing.KindDetails{Kind: typing.Array.Kind, OptionalArrayKind: &typing.Float},
		},
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			kind, err := PostgresDialect{}.KindForDataType(tt.dataType)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, kind)
		})
	}
}

func TestStripPrecision(t *testing.T) {
	{
		// Test with a string that has a precision
		s := "timestamp(6)"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, "timestamp", stripped)
		assert.Equal(t, "6", metadata)
	}
	{
		// Test with a string that doesn't have a precision
		s := "timestamp"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, s, stripped)
		assert.Empty(t, metadata)
	}
	{
		// Test with a string that has a precision but no parentheses
		s := "timestamp 6"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, "timestamp 6", stripped)
		assert.Empty(t, metadata)
	}
	{
		// Test with multiple precisions, it will only strip the first precision
		s := "timestamp(6) timestamp(3)"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, "timestamp timestamp(3)", stripped)
		assert.Equal(t, "6", metadata)
	}
	{
		s := "timestamp(6"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, s, stripped)
		assert.Empty(t, metadata)
	}
}

func TestDataTypeForKind(t *testing.T) {
	pd := PostgresDialect{}

	tests := []struct {
		name     string
		kd       typing.KindDetails
		isPk     bool
		settings config.SharedDestinationColumnSettings
		expected string
		wantErr  bool
	}{
		{
			name:     "float",
			kd:       typing.Float,
			expected: "double precision",
		},
		{
			name:     "boolean",
			kd:       typing.Boolean,
			expected: "boolean",
		},
		{
			name:     "struct",
			kd:       typing.Struct,
			expected: "jsonb",
		},
		{
			name:     "array",
			kd:       typing.Array,
			expected: "jsonb",
		},
		{
			name:     "string",
			kd:       typing.String,
			expected: "text",
		},
		{
			name:     "date",
			kd:       typing.Date,
			expected: "date",
		},
		{
			name:     "time",
			kd:       typing.TimeKindDetails,
			expected: "time",
		},
		{
			name:     "timestamp ntz",
			kd:       typing.TimestampNTZ,
			expected: "timestamp without time zone",
		},
		{
			name:     "timestamp tz",
			kd:       typing.TimestampTZ,
			expected: "timestamp with time zone",
		},
		{
			name: "integer default (nil kind)",
			kd: typing.KindDetails{
				Kind:                    typing.Integer.Kind,
				OptionalStringPrecision: nil,
			},
			expected: "bigint",
		},
		{
			name:     "integer default (no kind)",
			kd:       typing.BuildIntegerKind(typing.NotSpecifiedKind),
			expected: "bigint",
		},
		{
			name:     "smallint",
			kd:       typing.BuildIntegerKind(typing.SmallIntegerKind),
			expected: "smallint",
		},
		{
			name:     "integer",
			kd:       typing.BuildIntegerKind(typing.IntegerKind),
			expected: "integer",
		},
		{
			name:     "bigint",
			kd:       typing.BuildIntegerKind(typing.BigIntegerKind),
			expected: "bigint",
		},
		{
			name: "decimal with details",
			kd: typing.NewDecimalDetailsFromTemplate(
				typing.EDecimal,
				decimal.NewDetails(10, 2),
			),
			expected: "NUMERIC(10, 2)",
		},
		{
			name: "decimal missing details",
			kd: typing.KindDetails{
				Kind: typing.EDecimal.Kind,
			},
			wantErr: true,
		},
		{
			name: "unsupported kind",
			kd: typing.KindDetails{
				Kind: "unsupported_kind",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := pd.DataTypeForKind(tt.kd, tt.isPk, tt.settings)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}
