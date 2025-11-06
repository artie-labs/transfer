package motherduck

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
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
		assert.IsType(t, []interface{}{}, result)
		assert.Equal(t, input, result)
	}

	// Array as []string
	{
		input := []string{"red", "green", "blue"}
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)
		resultSlice := result.([]interface{})
		assert.Len(t, resultSlice, 3)
		assert.Equal(t, "red", resultSlice[0])
		assert.Equal(t, "green", resultSlice[1])
		assert.Equal(t, "blue", resultSlice[2])
	}

	// Array as JSON string
	{
		input := `["one","two","three"]`
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)
		resultSlice := result.([]interface{})
		assert.Len(t, resultSlice, 3)
		assert.Equal(t, "one", resultSlice[0])
		assert.Equal(t, "two", resultSlice[1])
		assert.Equal(t, "three", resultSlice[2])
	}

	// Array as JSON string with numbers
	{
		input := `[1,2,3,4,5]`
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)
		resultSlice := result.([]interface{})
		assert.Len(t, resultSlice, 5)
		assert.Equal(t, float64(1), resultSlice[0]) // JSON numbers parse as float64
		assert.Equal(t, float64(2), resultSlice[1])
	}

	// Array as JSON string with mixed types
	{
		input := `["text",123,true,null]`
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)
		resultSlice := result.([]interface{})
		assert.Len(t, resultSlice, 4)
		assert.Equal(t, "text", resultSlice[0])
		assert.Equal(t, float64(123), resultSlice[1])
		assert.Equal(t, true, resultSlice[2])
		assert.Nil(t, resultSlice[3])
	}

	// Array as non-JSON string (should wrap in single-element array)
	{
		input := "not a json array"
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)
		resultSlice := result.([]interface{})
		assert.Len(t, resultSlice, 1)
		assert.Equal(t, "not a json array", resultSlice[0])
	}

	// Empty array
	{
		input := []interface{}{}
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)
		assert.Len(t, result.([]interface{}), 0)
	}

	// Empty JSON array
	{
		input := `[]`
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)
		assert.Len(t, result.([]interface{}), 0)
	}

	// Nested arrays
	{
		input := `[["a","b"],["c","d"]]`
		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)
		resultSlice := result.([]interface{})
		assert.Len(t, resultSlice, 2)

		firstNested := resultSlice[0].([]interface{})
		assert.Len(t, firstNested, 2)
		assert.Equal(t, "a", firstNested[0])
		assert.Equal(t, "b", firstNested[1])
	}
}

func TestConvertValue_ArrayRoundTrip(t *testing.T) {
	// This test verifies that arrays maintain their integrity through conversion
	// This is critical for the fix of the "cannot cast string to []interface{}" error

	// Simple string array
	{
		input := []string{"alpha", "beta", "gamma"}
		expected := []interface{}{"alpha", "beta", "gamma"}

		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)

		resultSlice := result.([]interface{})
		assert.Equal(t, len(expected), len(resultSlice))
		for i, expectedVal := range expected {
			assert.Equal(t, expectedVal, resultSlice[i])
		}
	}

	// Interface array
	{
		input := []interface{}{"one", 2, true}
		expected := []interface{}{"one", 2, true}

		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)

		resultSlice := result.([]interface{})
		assert.Equal(t, len(expected), len(resultSlice))
		for i, expectedVal := range expected {
			assert.Equal(t, expectedVal, resultSlice[i])
		}
	}

	// JSON array string
	{
		input := `["foo","bar","baz"]`
		expected := []interface{}{"foo", "bar", "baz"}

		result, err := convertValue(input, typing.Array)
		assert.NoError(t, err)
		assert.IsType(t, []interface{}{}, result)

		resultSlice := result.([]interface{})
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
		// DuckDB appender accepts []interface{} as driver.Value
		assert.IsType(t, []interface{}{}, result)
	}

	// Nil returns nil driver.Value
	{
		result, err := convertValue(nil, typing.String)
		assert.NoError(t, err)
		assert.Nil(t, result)
	}
}
