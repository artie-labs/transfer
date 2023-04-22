package optimization

import (
	"github.com/artie-labs/transfer/lib/typing/ext"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestTableData_UpdateInMemoryColumns(t *testing.T) {
	var _cols typing.Columns
	for colName, colKind := range map[string]typing.KindDetails{
		"FOO":                  typing.String,
		"bar":                  typing.Invalid,
		"CHANGE_me":            typing.String,
		"do_not_change_format": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
	} {
		_cols.AddColumn(typing.Column{
			Name:        colName,
			KindDetails: colKind,
		})
	}

	tableData := &TableData{
		InMemoryColumns: &_cols,
	}

	extCol, isOk := tableData.InMemoryColumns.GetColumn("do_not_change_format")
	assert.True(t, isOk)

	extCol.KindDetails.ExtendedTimeDetails.Format = time.RFC3339Nano
	tableData.InMemoryColumns.UpdateColumn(typing.Column{
		Name:        extCol.Name,
		KindDetails: extCol.KindDetails,
	})

	for name, colKindDetails := range map[string]typing.KindDetails{
		"foo":                  typing.String,
		"change_me":            typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"bar":                  typing.Boolean,
		"do_not_change_format": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	} {
		tableData.UpdateInMemoryColumns(typing.Column{
			Name:        name,
			KindDetails: colKindDetails,
		})
	}

	// It's saved back in the original format.
	_, isOk = tableData.InMemoryColumns.GetColumn("foo")
	assert.True(t, isOk)

	_, isOk = tableData.InMemoryColumns.GetColumn("FOO")
	assert.True(t, isOk)

	col, isOk := tableData.InMemoryColumns.GetColumn("CHANGE_me")
	assert.True(t, isOk)
	assert.Equal(t, ext.DateTime.Type, col.KindDetails.ExtendedTimeDetails.Type)

	col, isOk = tableData.InMemoryColumns.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, typing.Invalid, col.KindDetails)

	col, isOk = tableData.InMemoryColumns.GetColumn("do_not_change_format")
	assert.True(t, isOk)
	assert.Equal(t, col.KindDetails.Kind, typing.ETime.Kind)
	assert.Equal(t, col.KindDetails.ExtendedTimeDetails.Type, ext.DateTimeKindType, "correctly mapped type")
	assert.Equal(t, col.KindDetails.ExtendedTimeDetails.Format, time.RFC3339Nano, "format has been preserved")
}
