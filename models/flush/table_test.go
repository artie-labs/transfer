package flush

import (
	"github.com/artie-labs/transfer/lib/typing/ext"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestTableData_UpdateInMemoryColumns(t *testing.T) {
	tableData := &TableData{
		InMemoryColumns: map[string]typing.KindDetails{
			"FOO":                  typing.String,
			"bar":                  typing.Invalid,
			"CHANGE_me":            typing.String,
			"do_not_change_format": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		},
	}

	tableData.InMemoryColumns["do_not_change_format"].ExtendedTimeDetails.Format = time.RFC3339Nano

	tableData.UpdateInMemoryColumns(map[string]typing.KindDetails{
		"foo":                  typing.String,
		"change_me":            typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"bar":                  typing.Boolean,
		"do_not_change_format": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	})

	// It's saved back in the original format.
	_, isOk := tableData.InMemoryColumns["foo"]
	assert.False(t, isOk)

	_, isOk = tableData.InMemoryColumns["FOO"]
	assert.True(t, isOk)

	colType, _ := tableData.InMemoryColumns["CHANGE_me"]
	assert.Equal(t, ext.DateTime.Type, colType.ExtendedTimeDetails.Type)

	colType, _ = tableData.InMemoryColumns["bar"]
	assert.Equal(t, typing.Invalid, colType)

	colType, _ = tableData.InMemoryColumns["do_not_change_format"]
	assert.Equal(t, colType.Kind, typing.ETime.Kind)
	assert.Equal(t, colType.ExtendedTimeDetails.Type, ext.DateTimeKindType, "correctly mapped type")
	assert.Equal(t, colType.ExtendedTimeDetails.Format, time.RFC3339Nano, "format has been preserved")
}
