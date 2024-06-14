package bigquery

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
)

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
		columns.NewColumn("c_int32", typing.Integer),
		columns.NewColumn("c_int64", typing.Integer),
		columns.NewColumn("c_float32", typing.Float),
		columns.NewColumn("c_float64", typing.Float),
		columns.NewColumn("c_numeric", typing.EDecimal),
		columns.NewColumn("c_string", typing.String),
		columns.NewColumn("c_array", typing.Array),
		columns.NewColumn("c_struct", typing.Struct),
	}

	row := map[string]any{
		"c_bool":    true,
		"c_int32":   int32(1234),
		"c_int64":   int32(1234),
		"c_float32": float32(1234.567),
		"c_float64": float64(1234.567),
		"c_numeric": decimal.NewDecimal(nil, 5, big.NewFloat(3.1415926)),
		"c_string":  "foo bar",
		"c_array":   []string{"foo", "bar"},
		"c_struct":  map[string]any{"baz": []string{"foo", "bar"}},
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
		"cBool":    true,
		"cFloat32": 1234.5670166015625,
		"cFloat64": 1234.567,
		"cInt32":   "1234",
		"cInt64":   "1234",
		"cString":  "foo bar",
		"cNumeric": "3.14159",
		"cArray":   []any{"foo", "bar"},
		"cStruct":  `{"baz":["foo","bar"]}`,
	}, result)
}
