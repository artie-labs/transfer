package bigquery

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
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
		// ETime - Time:
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType)))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_TIME, fieldSchema.Type)
	}
	{
		// ETime - Date:
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType)))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_DATE, fieldSchema.Type)
	}
	{
		// ETime - DateTime:
		fieldSchema, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)))
		assert.NoError(t, err)
		assert.Equal(t, storagepb.TableFieldSchema_TIMESTAMP, fieldSchema.Type)
	}
	{
		// ETime - Invalid:
		_, err := columnToTableFieldSchema(columns.NewColumn("foo", typing.NewKindDetailsFromTemplate(typing.ETime, "")))
		assert.ErrorContains(t, err, "unsupported extended time details type:")
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
		columns.NewColumn("c_time", typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType)),
		columns.NewColumn("c_date", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType)),
		columns.NewColumn("c_datetime", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)),
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
		"c_numeric":        decimal.NewDecimal(nil, 5, big.NewFloat(3.1415926)),
		"c_string":         "foo bar",
		"c_string_decimal": decimal.NewDecimal(nil, 5, big.NewFloat(1.618033)),
		"c_time":           ext.NewExtendedTime(time.Date(0, 0, 0, 4, 5, 6, 7, time.UTC), ext.TimeKindType, ""),
		"c_date":           ext.NewExtendedTime(time.Date(2001, 2, 3, 0, 0, 0, 0, time.UTC), ext.DateKindType, ""),
		"c_datetime":       ext.NewExtendedTime(time.Date(2001, 2, 3, 4, 5, 6, 7, time.UTC), ext.DateTimeKindType, ""),
		"c_struct":         map[string]any{"baz": []string{"foo", "bar"}},
		"c_array":          []string{"foo", "bar"},
	}

	desc, err := columnsToMessageDescriptor(columns)
	assert.NoError(t, err)

	message, err := rowToMessage(row, columns, *desc, []string{})
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
		"cDate":          float64(11356),
		"cDatetime":      "981173106000000",
		"cStruct":        `{"baz":["foo","bar"]}`,
		"cArray":         []any{"foo", "bar"},
	}, result)
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
		// Toasted map (should not happen):
		result, err := encodeStructToJSONString(map[string]any{"__debezium_unavailable_value": "bar", "baz": 1234})
		assert.NoError(t, err)
		assert.Equal(t, `{"key":"__debezium_unavailable_value"}`, result)
	}
}
