package motherduck

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/motherduck/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestConvertValue(t *testing.T) {
	// Nil value
	{
		result, err := convertValue(nil, typing.String)
		assert.NoError(t, err)
		assert.Nil(t, result)
	}

	// String type
	{
		result, err := convertValue("hello world", typing.String)
		assert.NoError(t, err)
		assert.Equal(t, "hello world", result)
	}
	{
		// Non-string value for string type should fail
		_, err := convertValue(123, typing.String)
		assert.Error(t, err)
	}

	// Boolean type
	{
		result, err := convertValue(true, typing.Boolean)
		assert.NoError(t, err)
		assert.Equal(t, true, result)
	}
	{
		result, err := convertValue(false, typing.Boolean)
		assert.NoError(t, err)
		assert.Equal(t, false, result)
	}
	{
		// Non-boolean value for boolean type should fail
		_, err := convertValue("true", typing.Boolean)
		assert.Error(t, err)
	}

	// Integer type
	{
		result, err := convertValue(42, typing.BuildIntegerKind(typing.IntegerKind))
		assert.NoError(t, err)
		assert.Equal(t, 42, result)
	}
	{
		result, err := convertValue(int64(9223372036854775807), typing.BuildIntegerKind(typing.BigIntegerKind))
		assert.NoError(t, err)
		assert.Equal(t, int64(9223372036854775807), result)
	}

	// Float type
	{
		result, err := convertValue(3.14, typing.Float)
		assert.NoError(t, err)
		assert.Equal(t, 3.14, result)
	}
	{
		result, err := convertValue(float32(2.5), typing.Float)
		assert.NoError(t, err)
		assert.Equal(t, float32(2.5), result)
	}

	// Decimal type - using string representation
	{
		result, err := convertValue("123.45", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(10, 2)))
		assert.NoError(t, err)
		assert.IsType(t, "", result) // Should be string
	}

	// Date type
	{
		result, err := convertValue("2024-01-15", typing.Date)
		assert.NoError(t, err)
		assert.IsType(t, time.Time{}, result)
	}
	{
		now := time.Now()
		result, err := convertValue(now, typing.Date)
		assert.NoError(t, err)
		assert.IsType(t, time.Time{}, result)
	}

	// Time type
	{
		result, err := convertValue("14:30:00", typing.Time)
		assert.NoError(t, err)
		assert.IsType(t, time.Time{}, result)
	}

	// Timestamp NTZ
	{
		result, err := convertValue("2024-01-15T14:30:00", typing.TimestampNTZ)
		assert.NoError(t, err)
		assert.IsType(t, time.Time{}, result)
	}

	// Timestamp TZ
	{
		result, err := convertValue("2024-01-15T14:30:00Z", typing.TimestampTZ)
		assert.NoError(t, err)
		assert.IsType(t, time.Time{}, result)
	}
}

func TestConvertValue_Struct(t *testing.T) {
	// Struct as map
	{
		input := map[string]interface{}{
			"name": "Alice",
			"age":  30,
		}
		result, err := convertValue(input, typing.Struct)
		assert.NoError(t, err)
		assert.IsType(t, "", result)
		assert.Contains(t, result.(string), "Alice")
		assert.Contains(t, result.(string), "30")
	}

	// Struct as JSON string - values.ToString will JSON-encode it again
	{
		input := `{"name":"Bob","age":25}`
		result, err := convertValue(input, typing.Struct)
		assert.NoError(t, err)
		// The string gets JSON-encoded by values.ToString(), so it's quoted
		assert.IsType(t, "", result)
		assert.Contains(t, result.(string), "Bob")
	}

	// Empty struct
	{
		input := map[string]interface{}{}
		result, err := convertValue(input, typing.Struct)
		assert.NoError(t, err)
		assert.IsType(t, "", result)
	}
}

func TestConvertValue_Array(t *testing.T) {
	// Array as []interface{}
	{
		input := []interface{}{"apple", "banana", "cherry"}
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []string{}, result)
		assert.Equal(t, []string{"apple", "banana", "cherry"}, result)
	}

	// Array as []string
	{
		input := []string{"red", "green", "blue"}
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []string{}, result)
		resultSlice := result.([]string)
		assert.Len(t, resultSlice, 3)
		assert.Equal(t, "red", resultSlice[0])
		assert.Equal(t, "green", resultSlice[1])
		assert.Equal(t, "blue", resultSlice[2])
	}

	// Empty array
	{
		input := []interface{}{}
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []string{}, result)
		assert.Len(t, result.([]string), 0)
	}
}

func TestConvertValue_ArrayRoundTrip(t *testing.T) {
	// This test verifies that arrays maintain their integrity through conversion
	// Now arrays are converted to []string for DuckDB's text[] columns

	// Simple string array
	{
		input := []string{"alpha", "beta", "gamma"}
		expected := []string{"alpha", "beta", "gamma"}

		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []string{}, result)

		resultSlice := result.([]string)
		assert.Equal(t, len(expected), len(resultSlice))
		for i, expectedVal := range expected {
			assert.Equal(t, expectedVal, resultSlice[i])
		}
	}

	// Interface array - all elements converted to strings
	{
		input := []interface{}{"one", 2, true}
		expected := []string{"one", "2", "true"}

		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []string{}, result)

		resultSlice := result.([]string)
		assert.Equal(t, len(expected), len(resultSlice))
		for i, expectedVal := range expected {
			assert.Equal(t, expectedVal, resultSlice[i])
		}
	}
}

func TestConvertValue_DriverValue(t *testing.T) {
	// Ensure returned values are valid driver.Value types

	// String returns driver.Value
	{
		result, err := convertValue("test", typing.String)
		assert.NoError(t, err)
		assert.IsType(t, "", result)
	}

	// Boolean returns driver.Value
	{
		result, err := convertValue(true, typing.Boolean)
		assert.NoError(t, err)
		assert.IsType(t, true, result)
	}

	// Array returns driver.Value
	{
		result, err := convertValue([]string{"a", "b"}, typing.Array)
		assert.NoError(t, err)
		// DuckDB appender accepts []string for text[] columns
		assert.IsType(t, []string{}, result)
	}

	// Nil returns nil driver.Value
	{
		result, err := convertValue(nil, typing.String)
		assert.NoError(t, err)
		assert.Nil(t, result)
	}
}

func TestCreateTempTable_ColumnOrder(t *testing.T) {
	// This test verifies that temporary tables are created with columns in the EXACT order
	// from tableData.ReadOnlyInMemoryCols().GetColumns() - not destination table order.
	// This is critical because the DuckDB append API is positional.

	// Create in-memory columns in a specific order that might differ from destination
	inMemoryCols := &columns.Columns{}
	inMemoryCols.AddColumn(columns.NewColumn("id", typing.Integer))
	inMemoryCols.AddColumn(columns.NewColumn("name", typing.String))
	inMemoryCols.AddColumn(columns.NewColumn("created_at", typing.TimestampTZ))
	inMemoryCols.AddColumn(columns.NewColumn("__artie_updated_at", typing.TimestampTZ))
	inMemoryCols.AddColumn(columns.NewColumn("__artie_operation", typing.String))

	tableData := optimization.NewTableData(
		inMemoryCols,
		config.Replication,
		[]string{"id"},
		kafkalib.TopicConfig{},
		"test_table",
	)

	// Get the columns that would be used for temp table creation
	tempTableCols := tableData.ReadOnlyInMemoryCols().GetColumns()

	// Build the CREATE TABLE SQL using MotherDuck dialect
	tableID := dialect.NewTableIdentifier("test_db", "test_schema", "temp__artie_test")
	createSQL, err := ddl.BuildCreateTableSQL(
		config.SharedDestinationColumnSettings{},
		dialect.DuckDBDialect{},
		tableID,
		true, // temporaryTable = true
		config.Replication,
		tempTableCols,
	)
	assert.NoError(t, err)

	// Verify the SQL has columns in the EXACT order from in-memory columns
	// The CREATE statement should look like: CREATE TABLE ... ("id" ..., "name" ..., "created_at" ..., "__artie_updated_at" ..., "__artie_operation" ...)
	expectedSQL := `CREATE TABLE "test_db"."test_schema"."temp__artie_test" ("id" bigint,"name" text,"created_at" timestamp with time zone,"__artie_updated_at" timestamp with time zone,"__artie_operation" text);`
	assert.Equal(t, expectedSQL, createSQL,
		"Temp table CREATE statement must have columns in the exact order from tableData.ReadOnlyInMemoryCols().GetColumns()")

	// Also verify the column order explicitly
	expectedOrder := []string{"id", "name", "created_at", "__artie_updated_at", "__artie_operation"}
	actualOrder := make([]string, 0)
	for _, col := range tempTableCols {
		if !col.ShouldSkip() {
			actualOrder = append(actualOrder, col.Name())
		}
	}
	assert.Equal(t, expectedOrder, actualOrder,
		"Column order must match tableData.ReadOnlyInMemoryCols().GetColumns() order")
}

func TestCreateTempTable_ColumnOrderDiffersFromDestination(t *testing.T) {
	// This is a regression test for the bug where __artie_operation value was appearing
	// in __artie_updated_at position. This happens when temp table column order doesn't
	// match the order used during append.

	// Simulate a scenario where destination table has a different column order
	inMemoryCols := &columns.Columns{}
	inMemoryCols.AddColumn(columns.NewColumn("col_a", typing.String))
	inMemoryCols.AddColumn(columns.NewColumn("col_b", typing.Integer))
	inMemoryCols.AddColumn(columns.NewColumn("__artie_updated_at", typing.TimestampTZ))
	inMemoryCols.AddColumn(columns.NewColumn("__artie_operation", typing.String))

	// Destination might have different order (e.g., alphabetical from DESCRIBE)
	destCols := []columns.Column{
		columns.NewColumn("__artie_updated_at", typing.TimestampTZ),
		columns.NewColumn("col_a", typing.String),
		columns.NewColumn("col_b", typing.Integer),
	}

	tableData := optimization.NewTableData(
		inMemoryCols,
		config.Replication,
		[]string{"col_a"},
		kafkalib.TopicConfig{},
		"test_table",
	)

	// Temp table columns should be from in-memory, NOT destination
	tempTableCols := tableData.ReadOnlyInMemoryCols().GetColumns()
	tempTableID := dialect.NewTableIdentifier("db", "schema", "temp__artie_test")

	tempSQL, err := ddl.BuildCreateTableSQL(
		config.SharedDestinationColumnSettings{},
		dialect.DuckDBDialect{},
		tempTableID,
		true,
		config.Replication,
		tempTableCols,
	)
	assert.NoError(t, err)

	// Verify temp table uses in-memory order
	assert.Contains(t, tempSQL, `"col_a" text,"col_b" bigint,"__artie_updated_at" timestamp with time zone,"__artie_operation" text`,
		"Temp table should use in-memory column order (col_a, col_b, __artie_updated_at, __artie_operation)")

	// If someone mistakenly uses destination order, it would look different
	wrongSQL, err := ddl.BuildCreateTableSQL(
		config.SharedDestinationColumnSettings{},
		dialect.DuckDBDialect{},
		tempTableID,
		true,
		config.Replication,
		destCols,
	)
	assert.NoError(t, err)

	// This should be DIFFERENT (wrong order)
	assert.NotEqual(t, tempSQL, wrongSQL,
		"Using destination column order would create a different table structure - should have been fixed")

	// The wrong SQL would have __artie_updated_at first
	assert.Contains(t, wrongSQL, `"__artie_updated_at" timestamp with time zone,"col_a" text,"col_b" bigint`,
		"Wrong order would have __artie_updated_at first, causing value misalignment during append")
}
