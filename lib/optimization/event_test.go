package optimization

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestTableData_UpdateInMemoryColumns(t *testing.T) {
	tableData := &TableData{
		InMemoryColumns: map[string]typing.KindDetails{
			"FOO":       typing.String,
			"bar":       typing.Invalid,
			"CHANGE_me": typing.String,
		},
	}

	tableData.UpdateInMemoryColumns(map[string]typing.KindDetails{
		"foo":       typing.String,
		"change_me": typing.NewKindDetailsFromTemplate(typing.ETime, typing.DateTimeKindType),
		"bar":       typing.Boolean,
	})

	// It's saved back in the original format.
	_, isOk := tableData.InMemoryColumns["foo"]
	assert.False(t, isOk)

	_, isOk = tableData.InMemoryColumns["FOO"]
	assert.True(t, isOk)

	colType, _ := tableData.InMemoryColumns["CHANGE_me"]
	assert.Equal(t, typing.DateTime.Type, colType.ExtendedTimeDetails.Type)

	colType, _ = tableData.InMemoryColumns["bar"]
	assert.Equal(t, typing.Invalid, colType)
}
