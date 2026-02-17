package bigquery

import (
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestColumnToTableFieldSchema(t *testing.T) {
	{
		// Boolean:
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.Boolean))
		assert.NoError(t, err)
		assert.Equal(t, "foo", fieldSchema.Name)
		assert.Equal(t, storagepb.TableFieldSchema_NULLABLE, fieldSchema.Mode)
		assert.Equal(t, storagepb.TableFieldSchema_BOOL, fieldSchema.Type)
	}
	{
		// Integer:
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.Integer))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_INT64, fieldSchema.Type)
	}
	{
		// Float:
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.Float))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_DOUBLE, fieldSchema.Type)
	}
	{
		// EDecimal:
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.EDecimal))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_STRING, fieldSchema.Type)
	}
	{
		// Time
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.TimeKindDetails))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_TIME, fieldSchema.Type)
	}
	{
		// Date
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.Date))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_DATE, fieldSchema.Type)
	}
	{
		// Datetime (TimestampNTZ)
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.TimestampNTZ))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_DATETIME, fieldSchema.Type)
	}
	{
		// Timestamp (TimestampTZ)
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.TimestampTZ))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_TIMESTAMP, fieldSchema.Type)
	}
	{
		// Struct:
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.Struct))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_STRING, fieldSchema.Type)
	}
	{
		// Array:
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.Array))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_STRING, fieldSchema.Type)
		assert.Equal(t, storagepb.TableFieldSchema_REPEATED, fieldSchema.Mode)
	}
	{
		// Invalid:
		_, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.KindDetails{}))
		assert.ErrorContains(t, err, "unsupported column kind: ")
	}
}

func TestEncodePacked64TimeMicros(t *testing.T) {
	epoch := time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)

	assert.Equal(t, int64(0), encodePacked64TimeMicros(epoch))
	assert.Equal(t, int64(1), encodePacked64TimeMicros(epoch.Add(time.Duration(1)*time.Microsecond)))
	assert.Equal(t, int64(1000), encodePacked64TimeMicros(epoch.Add(time.Duration(1)*time.Millisecond)))
	assert.Equal(t, int64(1<<20), encodePacked64TimeMicros(epoch.Add(time.Duration(1)*time.Second)))
	assert.Equal(t, int64(1<<26), encodePacked64TimeMicros(epoch.Add(time.Duration(1)*time.Minute)))
	assert.Equal(t, int64(1<<32), encodePacked64TimeMicros(epoch.Add(time.Duration(1)*time.Hour)))
	assert.Equal(t, int64(1<<32+1), encodePacked64TimeMicros(epoch.Add(time.Duration(1)*time.Hour+time.Duration(1)*time.Microsecond)))
	assert.Equal(t, int64(1<<32+1000), encodePacked64TimeMicros(epoch.Add(time.Duration(1)*time.Hour+time.Duration(1)*time.Millisecond)))
}

func TestEncodePacked32TimeSeconds(t *testing.T) {
	epoch := time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)

	assert.Equal(t, int32(0), encodePacked32TimeSeconds(epoch))
	assert.Equal(t, int32(1), encodePacked32TimeSeconds(epoch.Add(time.Duration(1)*time.Second)))
	assert.Equal(t, int32(1<<6), encodePacked32TimeSeconds(epoch.Add(time.Duration(1)*time.Minute)))
	assert.Equal(t, int32(1<<12), encodePacked32TimeSeconds(epoch.Add(time.Duration(1)*time.Hour)))
	assert.Equal(t, int32(1<<12+1), encodePacked32TimeSeconds(epoch.Add(time.Duration(1)*time.Hour+time.Duration(1)*time.Second)))
}

func TestEncodePacked64DatetimeSeconds(t *testing.T) {
	ts := time.Date(2024, 10, 24, 13, 1, 2, 3000000, time.UTC)
	expected := 2024<<26 + 10<<22 + 24<<17 + int64(encodePacked32TimeSeconds(ts))

	// Time
	assert.Equal(t, expected, encodePacked64DatetimeSeconds(ts))
	assert.Equal(t, expected+1<<0, encodePacked64DatetimeSeconds(ts.Add(time.Duration(1)*time.Second)))
	assert.Equal(t, expected+1<<6, encodePacked64DatetimeSeconds(ts.Add(time.Duration(1)*time.Minute)))
	assert.Equal(t, expected+1<<12, encodePacked64DatetimeSeconds(ts.Add(time.Duration(1)*time.Hour)))
	assert.Equal(t, expected+1<<12+1<<0, encodePacked64DatetimeSeconds(ts.Add(time.Duration(1)*time.Hour+time.Duration(1)*time.Second)))

	// Day
	assert.Equal(t, expected+1<<17, encodePacked64DatetimeSeconds(ts.Add(time.Duration(24)*time.Hour)))
	// Month
	assert.Equal(t, expected+1<<22, encodePacked64DatetimeSeconds(ts.AddDate(0, 1, 0)))
	// Year
	assert.Equal(t, expected+1<<26, encodePacked64DatetimeSeconds(ts.AddDate(1, 0, 0)))
	// Month and year
	assert.Equal(t, expected+1<<22+1<<26, encodePacked64DatetimeSeconds(ts.AddDate(1, 1, 0)))
}

func TestEncodePacked64DatetimeMicros(t *testing.T) {
	ts := time.Date(2024, 10, 24, 13, 1, 2, 123456789, time.UTC)
	expected := encodePacked64DatetimeSeconds(ts)<<20 | int64(ts.Nanosecond()/1000)

	// Time
	assert.Equal(t, expected, encodePacked64DatetimeMicros(ts))
	assert.Equal(t, expected+1<<(0+20), encodePacked64DatetimeMicros(ts.Add(time.Duration(1)*time.Second)))
	assert.Equal(t, expected+1<<(6+20), encodePacked64DatetimeMicros(ts.Add(time.Duration(1)*time.Minute)))
	assert.Equal(t, expected+1<<(12+20), encodePacked64DatetimeMicros(ts.Add(time.Duration(1)*time.Hour)))
	assert.Equal(t, expected+1<<(12+20)+1<<(0+20), encodePacked64DatetimeMicros(ts.Add(time.Duration(1)*time.Hour+time.Duration(1)*time.Second)))

	// Day
	assert.Equal(t, expected+1<<(17+20), encodePacked64DatetimeMicros(ts.Add(time.Duration(24)*time.Hour)))
	// Month
	assert.Equal(t, expected+1<<(22+20), encodePacked64DatetimeMicros(ts.AddDate(0, 1, 0)))
	// Year
	assert.Equal(t, expected+1<<(26+20), encodePacked64DatetimeMicros(ts.AddDate(1, 0, 0)))
	// Month and year
	assert.Equal(t, expected+1<<(26+20)+1<<(22+20), encodePacked64DatetimeMicros(ts.AddDate(1, 1, 0)))
}

func TestRowToMessage(t *testing.T) {
	columns := []columns.Column{
		columns.NewColumn("c_bool", typing.Boolean),
		columns.NewColumn("c_int", typing.Integer),
		columns.NewColumn("c_int32", typing.Integer),
		columns.NewColumn("c_int64", typing.Integer),
		columns.NewColumn("c_float32", typing.Float),
		columns.NewColumn("c_float64", typing.Float),
		columns.NewColumn("c_float_int32", typing.Float),
		columns.NewColumn("c_float_int64", typing.Float),
		columns.NewColumn("c_float_string", typing.Float),
		columns.NewColumn("c_numeric", typing.EDecimal),
		columns.NewColumn("c_string", typing.String),
		columns.NewColumn("c_string_decimal", typing.String),
		columns.NewColumn("c_time", typing.TimeKindDetails),
		columns.NewColumn("c_timestamp", typing.TimestampTZ),
		columns.NewColumn("c_date", typing.Date),
		columns.NewColumn("c_datetime", typing.TimestampNTZ),
		columns.NewColumn("c_struct", typing.Struct),
		columns.NewColumn("c_array", typing.Array),
	}

	row := map[string]any{
		"c_bool":           true,
		"c_int":            int(1234),
		"c_int32":          int32(1234),
		"c_int64":          int64(1234),
		"c_float32":        float32(1234.567),
		"c_float64":        float64(1234.567),
		"c_float_int32":    int32(1234),
		"c_float_int64":    int64(1234),
		"c_float_string":   "4444.55555",
		"c_numeric":        decimal.NewDecimal(numbers.MustParseDecimal("3.14159")),
		"c_string":         "foo bar",
		"c_string_decimal": decimal.NewDecimal(numbers.MustParseDecimal("1.61803")),
		"c_time":           time.Date(0, 0, 0, 4, 5, 6, 7, time.UTC),
		"c_timestamp":      time.Date(2001, 2, 3, 4, 5, 6, 7, time.UTC),
		"c_date":           time.Date(2001, 2, 3, 0, 0, 0, 0, time.UTC),
		"c_datetime":       time.Date(2001, 2, 3, 4, 5, 6, 7, time.UTC),
		"c_struct":         map[string]any{"baz": []string{"foo", "bar"}},
		"c_array":          []string{"foo", "bar"},
	}

	desc, err := columnsToMessageDescriptor(columns)
	assert.NoError(t, err)

	message, err := rowToMessage(row, columns, *desc, config.Config{})
	assert.NoError(t, err)

	bytes, err := protojson.Marshal(message)
	assert.NoError(t, err)

	var result map[string]any
	assert.NoError(t, json.Unmarshal(bytes, &result))

	assert.Equal(t, map[string]any{
		"cBool":          true,
		"cFloat32":       1234.5670166015625,
		"cFloat64":       1234.567,
		"cFloatInt32":    1234.0,
		"cFloatInt64":    1234.0,
		"cFloatString":   4444.55555,
		"cInt":           "1234",
		"cInt32":         "1234",
		"cInt64":         "1234",
		"cNumeric":       "3.14159",
		"cString":        "foo bar",
		"cStringDecimal": "1.61803",
		"cTime":          "17521704960",
		"cTimestamp":     "981173106000000",
		"cDate":          float64(11356),
		"cDatetime":      "140817083031093248",
		"cStruct":        `{"baz":["foo","bar"]}`,
		"cArray":         []any{"foo", "bar"},
	}, result)
}

func TestRowToMessage_EmptyStringFloat(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("c_float", typing.Float),
	}

	desc, err := columnsToMessageDescriptor(cols)
	assert.NoError(t, err)

	message, err := rowToMessage(map[string]any{"c_float": ""}, cols, *desc, config.Config{})
	assert.NoError(t, err)

	bytes, err := protojson.Marshal(message)
	assert.NoError(t, err)

	var result map[string]any
	assert.NoError(t, json.Unmarshal(bytes, &result))
	assert.Empty(t, result) // Field should not be set when value is empty string
}

func TestRowToMessage_TruncateExceededDecimal(t *testing.T) {
	details := decimal.NewDetails(76, 38)
	cols := []columns.Column{
		columns.NewColumn("c_numeric", typing.KindDetails{
			Kind:                   typing.EDecimal.Kind,
			ExtendedDecimalDetails: &details,
		}),
	}

	desc, err := columnsToMessageDescriptor(cols)
	assert.NoError(t, err)

	{
		// Without TruncateExceededValues - value is passed through as-is
		message, err := rowToMessage(
			map[string]any{"c_numeric": "1.1234567890123456789012345678901234567890"},
			cols, *desc, config.Config{},
		)
		assert.NoError(t, err)

		bytes, err := protojson.Marshal(message)
		assert.NoError(t, err)

		var result map[string]any
		assert.NoError(t, json.Unmarshal(bytes, &result))
		assert.Equal(t, "1.1234567890123456789012345678901234567890", result["cNumeric"])
	}
	{
		// With TruncateExceededValues - value is truncated to column's scale (38)
		message, err := rowToMessage(
			map[string]any{"c_numeric": "1.1234567890123456789012345678901234567890"},
			cols, *desc, config.Config{
				SharedDestinationSettings: config.SharedDestinationSettings{
					TruncateExceededValues: true,
				},
			},
		)
		assert.NoError(t, err)

		bytes, err := protojson.Marshal(message)
		assert.NoError(t, err)

		var result map[string]any
		assert.NoError(t, json.Unmarshal(bytes, &result))
		assert.Equal(t, "1.12345678901234567890123456789012345678", result["cNumeric"])
	}
}

func TestRowToMessage_TruncateExceededDecimal_UnspecifiedPrecision(t *testing.T) {
	// When ExtendedDecimalDetails has PrecisionNotSpecified (e.g., Postgres bare NUMERIC),
	// the code should fall through to the BQ-type-based max scale rather than using DefaultScale (5).
	notSetDetails := decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)
	cols := []columns.Column{
		columns.NewColumn("c_numeric", typing.KindDetails{
			Kind:                   typing.EDecimal.Kind,
			ExtendedDecimalDetails: &notSetDetails,
		}),
	}

	desc, err := columnsToMessageDescriptor(cols)
	assert.NoError(t, err)

	{
		// With TruncateExceededValues + BigNumericForVariableNumeric - should truncate to 38 (BIGNUMERIC max), not 5
		message, err := rowToMessage(
			map[string]any{"c_numeric": "1.1234567890123456789012345678901234567890"},
			cols, *desc, config.Config{
				SharedDestinationSettings: config.SharedDestinationSettings{
					TruncateExceededValues: true,
					ColumnSettings: config.SharedDestinationColumnSettings{
						BigNumericForVariableNumeric: true,
					},
				},
			},
		)
		assert.NoError(t, err)

		bytes, err := protojson.Marshal(message)
		assert.NoError(t, err)

		var result map[string]any
		assert.NoError(t, json.Unmarshal(bytes, &result))
		assert.Equal(t, "1.12345678901234567890123456789012345678", result["cNumeric"])
	}
	{
		// With TruncateExceededValues without BigNumericForVariableNumeric - should truncate to 9 (NUMERIC max), not 5
		message, err := rowToMessage(
			map[string]any{"c_numeric": "1.1234567890123456789012345678901234567890"},
			cols, *desc, config.Config{
				SharedDestinationSettings: config.SharedDestinationSettings{
					TruncateExceededValues: true,
				},
			},
		)
		assert.NoError(t, err)

		bytes, err := protojson.Marshal(message)
		assert.NoError(t, err)

		var result map[string]any
		assert.NoError(t, json.Unmarshal(bytes, &result))
		assert.Equal(t, "1.123456789", result["cNumeric"])
	}
}

func TestEncodeStructToJSONString(t *testing.T) {
	{
		// Empty string:
		result, err := encodeStructToJSONString("")
		assert.NoError(t, err)
		assert.Equal(t, "", result)
	}
	{
		// Toasted string:
		result, err := encodeStructToJSONString("__debezium_unavailable_value")
		assert.NoError(t, err)
		assert.Equal(t, `{"key":"__debezium_unavailable_value"}`, result)
	}
	{
		// Map:
		result, err := encodeStructToJSONString(map[string]any{"foo": "bar", "baz": 1234})
		assert.NoError(t, err)
		assert.Equal(t, `{"baz":1234,"foo":"bar"}`, result)
	}
	{
		// SQL query string with escaped newlines (not valid JSON):
		// This simulates the production issue where a SQL query string is written to a JSON column.
		sqlQuery := "WITH products_sold_last_30 AS (\\n  select distinct product_id as id from core_pos_order_line_items where created_at > CURRENT_DATE - INTERVAL '30 days'\\n)\\nSELECT\\n    sp.barcode as lookup_code"
		result, err := encodeStructToJSONString(sqlQuery)
		assert.NoError(t, err)
		// Should be properly JSON-escaped (not Go-quoted)
		// json.Marshal will produce a JSON string with proper escaping
		expected, _ := json.Marshal(sqlQuery)
		assert.Equal(t, string(expected), result)
		// Verify it's valid JSON
		assert.True(t, json.Valid([]byte(result)))
	}
}
