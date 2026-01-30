package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestGetOptionalSchema(t *testing.T) {
	{
		// MySQL
		{
			// Insert
			var schemaEventPayload SchemaEventPayload
			assert.NoError(t, json.Unmarshal([]byte(MySQLInsert), &schemaEventPayload))

			optionalSchema, err := schemaEventPayload.GetOptionalSchema(config.SharedDestinationSettings{})
			assert.NoError(t, err)
			assert.Equal(
				t,
				optionalSchema,
				map[string]typing.KindDetails{
					"id":         typing.Integer,
					"first_name": typing.String,
					"last_name":  typing.String,
					"email":      typing.String,
				},
			)
		}
		{
			// Update
			var schemaEventPayload SchemaEventPayload
			assert.NoError(t, json.Unmarshal([]byte(MySQLUpdate), &schemaEventPayload))

			optionalSchema, err := schemaEventPayload.GetOptionalSchema(config.SharedDestinationSettings{})
			assert.NoError(t, err)
			assert.Equal(
				t,
				optionalSchema,
				map[string]typing.KindDetails{
					"id":         typing.Integer,
					"first_name": typing.String,
					"last_name":  typing.String,
					"email":      typing.String,
				},
			)
		}
		{
			// Delete
			var schemaEventPayload SchemaEventPayload
			assert.NoError(t, json.Unmarshal([]byte(MySQLDelete), &schemaEventPayload))

			optionalSchema, err := schemaEventPayload.GetOptionalSchema(config.SharedDestinationSettings{})
			assert.NoError(t, err)
			assert.Equal(
				t,
				optionalSchema,
				map[string]typing.KindDetails{
					"id":         typing.Integer,
					"first_name": typing.String,
					"last_name":  typing.String,
					"email":      typing.String,
				},
			)
		}
	}
	{
		// Postgres
		{
			// Delete
			var schemaEventPayload SchemaEventPayload
			assert.NoError(t, json.Unmarshal([]byte(PostgresDelete), &schemaEventPayload))

			optionalSchema, err := schemaEventPayload.GetOptionalSchema(config.SharedDestinationSettings{})
			assert.NoError(t, err)
			assert.Equal(
				t,
				optionalSchema,
				map[string]typing.KindDetails{
					"id":         typing.Integer,
					"first_name": typing.String,
					"last_name":  typing.String,
					"email":      typing.String,
				},
			)
		}
		{
			// Update
			var schemaEventPayload SchemaEventPayload
			assert.NoError(t, json.Unmarshal([]byte(PostgresUpdate), &schemaEventPayload))

			optionalSchema, err := schemaEventPayload.GetOptionalSchema(config.SharedDestinationSettings{})
			assert.NoError(t, err)
			assert.Equal(
				t,
				optionalSchema,
				map[string]typing.KindDetails{
					"id":           typing.Integer,
					"first_name":   typing.String,
					"last_name":    typing.String,
					"email":        typing.String,
					"boolean_test": typing.Boolean,
					"bool_test":    typing.Boolean,
					"bit_test":     typing.Boolean,
					"numeric_test": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)),
					"numeric_5":    typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)),
					"numeric_5_0":  typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)),
					"numeric_5_2":  typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 2)),
					"numeric_5_6":  typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 6)),
					"numeric_39_0": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(39, 0)),
					"numeric_39_2": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(39, 2)),
					"numeric_39_6": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(39, 6)),
				},
			)
		}
	}
}
