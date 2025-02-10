package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestDataTypeForKind(t *testing.T) {
	_dialect := IcebergDialect{}

	// Boolean
	assert.Equal(t, "BOOLEAN", _dialect.DataTypeForKind(typing.Boolean, false, config.SharedDestinationColumnSettings{}))

	// String
	assert.Equal(t, "STRING", _dialect.DataTypeForKind(typing.String, false, config.SharedDestinationColumnSettings{}))

	{
		// Float
		assert.Equal(t, "DOUBLE", _dialect.DataTypeForKind(typing.Float, false, config.SharedDestinationColumnSettings{}))

		// EDecimal
		{
			// DECIMAL(5, 2)
			kd := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 2))
			assert.Equal(t, "DECIMAL(5, 2)", _dialect.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{}))
		}
		{
			// DECIMAL(39, 2) - Exceeds the max precision, so it will become a string.
			kd := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(39, 2))
			assert.Equal(t, "STRING", _dialect.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{}))
		}
	}

	// Array
	assert.Equal(t, "LIST", _dialect.DataTypeForKind(typing.Array, false, config.SharedDestinationColumnSettings{}))

	// Struct
	assert.Equal(t, "STRUCT", _dialect.DataTypeForKind(typing.Struct, false, config.SharedDestinationColumnSettings{}))

	// Date
	assert.Equal(t, "DATE", _dialect.DataTypeForKind(typing.Date, false, config.SharedDestinationColumnSettings{}))

	// Time
	assert.Equal(t, "TIME", _dialect.DataTypeForKind(typing.Time, false, config.SharedDestinationColumnSettings{}))

	// TimestampNTZ
	assert.Equal(t, "TIMESTAMP WITHOUT TIMEZONE", _dialect.DataTypeForKind(typing.TimestampNTZ, false, config.SharedDestinationColumnSettings{}))

	// TimestampTZ
	assert.Equal(t, "TIMESTAMP WITH TIMEZONE", _dialect.DataTypeForKind(typing.TimestampTZ, false, config.SharedDestinationColumnSettings{}))
}
