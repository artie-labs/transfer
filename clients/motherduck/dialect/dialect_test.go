package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestKindForDataType(t *testing.T) {
	dd := DuckDBDialect{}

	// Numbers with precision
	{
		kind, err := dd.KindForDataType("numeric(5, 2)")
		assert.NoError(t, err)
		assert.Equal(t, typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 2)), kind)
	}
	{
		kind, err := dd.KindForDataType("numeric(5, 0)")
		assert.NoError(t, err)
		assert.Equal(t, typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)), kind)
	}
	{
		kind, err := dd.KindForDataType("numeric(5)")
		assert.NoError(t, err)
		assert.Equal(t, typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)), kind)
	}

	// Variable numeric types
	{
		kind, err := dd.KindForDataType("numeric")
		assert.NoError(t, err)
		assert.Equal(t, typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)), kind)
	}
	{
		kind, err := dd.KindForDataType("decimal")
		assert.NoError(t, err)
		assert.Equal(t, typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)), kind)
	}

	// Float types
	{
		kind, err := dd.KindForDataType("float")
		assert.NoError(t, err)
		assert.Equal(t, typing.Float, kind)
	}
	{
		kind, err := dd.KindForDataType("double")
		assert.NoError(t, err)
		assert.Equal(t, typing.Float, kind)
	}

	// Small integer types
	{
		kind, err := dd.KindForDataType("smallint")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.SmallIntegerKind), kind)
	}
	{
		kind, err := dd.KindForDataType("int2")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.SmallIntegerKind), kind)
	}
	{
		kind, err := dd.KindForDataType("short")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.SmallIntegerKind), kind)
	}

	// Integer types
	{
		kind, err := dd.KindForDataType("integer")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.IntegerKind), kind)
	}
	{
		kind, err := dd.KindForDataType("int4")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.IntegerKind), kind)
	}
	{
		kind, err := dd.KindForDataType("int")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.IntegerKind), kind)
	}
	{
		kind, err := dd.KindForDataType("signed")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.IntegerKind), kind)
	}

	// Big integer types
	{
		kind, err := dd.KindForDataType("bigint")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.BigIntegerKind), kind)
	}
	{
		kind, err := dd.KindForDataType("int8")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.BigIntegerKind), kind)
	}
	{
		kind, err := dd.KindForDataType("long")
		assert.NoError(t, err)
		assert.Equal(t, typing.BuildIntegerKind(typing.BigIntegerKind), kind)
	}

	// String types
	{
		kind, err := dd.KindForDataType("text")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kind)
	}
	{
		kind, err := dd.KindForDataType("varchar")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kind)
	}
	{
		kind, err := dd.KindForDataType("char")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kind)
	}
	{
		kind, err := dd.KindForDataType("bpchar")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kind)
	}
	{
		kind, err := dd.KindForDataType("string")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kind)
	}

	// Boolean
	{
		kind, err := dd.KindForDataType("boolean")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kind)
	}

	// Date and time types
	{
		kind, err := dd.KindForDataType("date")
		assert.NoError(t, err)
		assert.Equal(t, typing.Date, kind)
	}
	{
		kind, err := dd.KindForDataType("time")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimeKindDetails, kind)
	}
	{
		kind, err := dd.KindForDataType("timestamp")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampNTZ, kind)
	}
	{
		kind, err := dd.KindForDataType("datetime")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampNTZ, kind)
	}
	{
		kind, err := dd.KindForDataType("timestamp with time zone")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampTZ, kind)
	}
	{
		kind, err := dd.KindForDataType("timestamptz")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampTZ, kind)
	}

	// Array types with [] notation
	{
		kind, err := dd.KindForDataType("text[]")
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kind)
	}
	{
		kind, err := dd.KindForDataType("integer[]")
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kind)
	}
	{
		kind, err := dd.KindForDataType("bigint[]")
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kind)
	}
	{
		kind, err := dd.KindForDataType("json[]")
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kind)
	}

	// Other complex types
	{
		kind, err := dd.KindForDataType("array")
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kind)
	}
	{
		kind, err := dd.KindForDataType("struct")
		assert.NoError(t, err)
		assert.Equal(t, typing.Struct, kind)
	}
	{
		kind, err := dd.KindForDataType("json")
		assert.NoError(t, err)
		assert.Equal(t, typing.Struct, kind)
	}
}

func TestDataTypeForKind(t *testing.T) {
	dd := DuckDBDialect{}

	// Float
	{
		result, err := dd.DataTypeForKind(typing.Float, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "double", result)
	}

	// Integer - not specified
	{
		result, err := dd.DataTypeForKind(typing.BuildIntegerKind(typing.NotSpecifiedKind), false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "bigint", result)
	}

	// Integer - small
	{
		result, err := dd.DataTypeForKind(typing.BuildIntegerKind(typing.SmallIntegerKind), false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "smallint", result)
	}

	// Integer - int
	{
		result, err := dd.DataTypeForKind(typing.BuildIntegerKind(typing.IntegerKind), false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "integer", result)
	}

	// Integer - big
	{
		result, err := dd.DataTypeForKind(typing.BuildIntegerKind(typing.BigIntegerKind), false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "bigint", result)
	}

	// Boolean
	{
		result, err := dd.DataTypeForKind(typing.Boolean, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "boolean", result)
	}

	// Array
	{
		result, err := dd.DataTypeForKind(typing.Array, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "text[]", result)
	}

	// Struct
	{
		result, err := dd.DataTypeForKind(typing.Struct, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "json", result)
	}

	// String
	{
		result, err := dd.DataTypeForKind(typing.String, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "text", result)
	}

	// Date
	{
		result, err := dd.DataTypeForKind(typing.Date, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "date", result)
	}

	// Time
	{
		result, err := dd.DataTypeForKind(typing.TimeKindDetails, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "time", result)
	}

	// Timestamp NTZ
	{
		result, err := dd.DataTypeForKind(typing.TimestampNTZ, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "timestamp", result)
	}

	// Timestamp TZ
	{
		result, err := dd.DataTypeForKind(typing.TimestampTZ, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "timestamp with time zone", result)
	}
}

func TestRoundTripConversion(t *testing.T) {
	// This test ensures that converting a Kind to a DataType and back to a Kind
	// returns the same Kind (no data loss in round-trip conversion)
	dd := DuckDBDialect{}

	// Array round trip
	{
		// Kind -> DataType
		dataType, err := dd.DataTypeForKind(typing.Array, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "text[]", dataType)

		// DataType -> Kind
		resultKind, err := dd.KindForDataType(dataType)
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, resultKind, "Round trip conversion failed for array")
	}

	// Struct round trip
	{
		// Kind -> DataType
		dataType, err := dd.DataTypeForKind(typing.Struct, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "json", dataType)

		// DataType -> Kind
		resultKind, err := dd.KindForDataType(dataType)
		assert.NoError(t, err)
		assert.Equal(t, typing.Struct, resultKind, "Round trip conversion failed for struct")
	}

	// String round trip
	{
		// Kind -> DataType
		dataType, err := dd.DataTypeForKind(typing.String, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "text", dataType)

		// DataType -> Kind
		resultKind, err := dd.KindForDataType(dataType)
		assert.NoError(t, err)
		assert.Equal(t, typing.String, resultKind, "Round trip conversion failed for string")
	}

	// Boolean round trip
	{
		// Kind -> DataType
		dataType, err := dd.DataTypeForKind(typing.Boolean, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "boolean", dataType)

		// DataType -> Kind
		resultKind, err := dd.KindForDataType(dataType)
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, resultKind, "Round trip conversion failed for boolean")
	}
}

func TestBuildDedupeQueries(t *testing.T) {
	dd := DuckDBDialect{}

	// Test with single primary key, no artie updated at
	{
		tableID := NewTableIdentifier("test_db", "public", "users")
		stagingTableID := NewTableIdentifier("test_db", "public", "users_staging")
		primaryKeys := []string{"id"}

		queries := dd.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, false)

		assert.Len(t, queries, 4, "Should return 4 queries")

		assert.Equal(t, `CREATE TABLE "test_db"."public"."users_staging" AS (SELECT * FROM "test_db"."public"."users" QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" DESC) = 1)`, queries[0])
		assert.Equal(t, `DELETE FROM "test_db"."public"."users" t1 WHERE EXISTS (SELECT 1 FROM "test_db"."public"."users_staging" t2 WHERE t1."id" = t2."id")`, queries[1])
		assert.Equal(t, `INSERT INTO "test_db"."public"."users" SELECT * FROM "test_db"."public"."users_staging"`, queries[2])
		assert.Equal(t, `DROP TABLE IF EXISTS "test_db"."public"."users_staging"`, queries[3])
	}

	// Test with multiple primary keys, no artie updated at
	{
		tableID := NewTableIdentifier("test_db", "public", "orders")
		stagingTableID := NewTableIdentifier("test_db", "public", "orders_staging")
		primaryKeys := []string{"order_id", "line_item_id"}

		queries := dd.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, false)

		assert.Len(t, queries, 4, "Should return 4 queries")

		assert.Equal(t, `CREATE TABLE "test_db"."public"."orders_staging" AS (SELECT * FROM "test_db"."public"."orders" QUALIFY ROW_NUMBER() OVER (PARTITION BY "order_id", "line_item_id" ORDER BY "order_id" DESC, "line_item_id" DESC) = 1)`, queries[0])
		assert.Equal(t, `DELETE FROM "test_db"."public"."orders" t1 WHERE EXISTS (SELECT 1 FROM "test_db"."public"."orders_staging" t2 WHERE t1."order_id" = t2."order_id" AND t1."line_item_id" = t2."line_item_id")`, queries[1])
		assert.Equal(t, `INSERT INTO "test_db"."public"."orders" SELECT * FROM "test_db"."public"."orders_staging"`, queries[2])
		assert.Equal(t, `DROP TABLE IF EXISTS "test_db"."public"."orders_staging"`, queries[3])
	}

	// Test with single primary key and includeArtieUpdatedAt = true
	{
		tableID := NewTableIdentifier("test_db", "public", "users")
		stagingTableID := NewTableIdentifier("test_db", "public", "users_staging")
		primaryKeys := []string{"id"}

		queries := dd.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, true)

		assert.Len(t, queries, 4, "Should return 4 queries")

		assert.Equal(t, `CREATE TABLE "test_db"."public"."users_staging" AS (SELECT * FROM "test_db"."public"."users" QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" DESC, "__artie_updated_at" DESC) = 1)`, queries[0])
		assert.Equal(t, `DELETE FROM "test_db"."public"."users" t1 WHERE EXISTS (SELECT 1 FROM "test_db"."public"."users_staging" t2 WHERE t1."id" = t2."id")`, queries[1])
		assert.Equal(t, `INSERT INTO "test_db"."public"."users" SELECT * FROM "test_db"."public"."users_staging"`, queries[2])
		assert.Equal(t, `DROP TABLE IF EXISTS "test_db"."public"."users_staging"`, queries[3])
	}

	// Test with multiple primary keys and includeArtieUpdatedAt = true
	{
		tableID := NewTableIdentifier("test_db", "public", "orders")
		stagingTableID := NewTableIdentifier("test_db", "public", "orders_staging")
		primaryKeys := []string{"order_id", "line_item_id"}

		queries := dd.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, true)

		assert.Len(t, queries, 4, "Should return 4 queries")

		assert.Equal(t, `CREATE TABLE "test_db"."public"."orders_staging" AS (SELECT * FROM "test_db"."public"."orders" QUALIFY ROW_NUMBER() OVER (PARTITION BY "order_id", "line_item_id" ORDER BY "order_id" DESC, "line_item_id" DESC, "__artie_updated_at" DESC) = 1)`, queries[0])
		assert.Equal(t, `DELETE FROM "test_db"."public"."orders" t1 WHERE EXISTS (SELECT 1 FROM "test_db"."public"."orders_staging" t2 WHERE t1."order_id" = t2."order_id" AND t1."line_item_id" = t2."line_item_id")`, queries[1])
		assert.Equal(t, `INSERT INTO "test_db"."public"."orders" SELECT * FROM "test_db"."public"."orders_staging"`, queries[2])
		assert.Equal(t, `DROP TABLE IF EXISTS "test_db"."public"."orders_staging"`, queries[3])
	}
}
