package optimization

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
		tableData.UpdateInMemoryColumnsFromDestination(typing.Column{
			Name:        name,
			KindDetails: colKindDetails,
		})
	}

	// It's saved back in the original format.
	_, isOk = tableData.InMemoryColumns.GetColumn("foo")
	assert.False(t, isOk)

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

func TestTableData_ShouldFlushRowLength(t *testing.T) {
	ctx := context.Background()
	ctx = config.InjectSettingsIntoContext(ctx, &config.Settings{Config: &config.Config{
		FlushSizeKb:          500,
		BufferRows:           2,
	}})

	// Insert 3 rows and confirm that we need to flush.
	td := NewTableData(nil, nil, kafkalib.TopicConfig{})
	for i := 0; i < 3; i ++ {
		assert.False(t, td.ShouldFlush(ctx))
		td.InsertRow(fmt.Sprint(i), map[string]interface{}{
			"foo": "bar",
		})
	}

	assert.True(t, td.ShouldFlush(ctx))
}

func TestTableData_ShouldFlushRowSize(t *testing.T) {
	ctx := context.Background()
	ctx = config.InjectSettingsIntoContext(ctx, &config.Settings{Config: &config.Config{
		FlushSizeKb:          5,
		BufferRows:           20000,
	}})

	// Insert 3 rows and confirm that we need to flush.
	td := NewTableData(nil, nil, kafkalib.TopicConfig{})
	for i := 0; i < 45; i ++ {
		assert.False(t, td.ShouldFlush(ctx))
		td.InsertRow(fmt.Sprint(i), map[string]interface{}{
			"foo": "bar",
			"array": []string{"foo", "bar", "dusty", "the aussie", "robin", "jacqueline", "charlie"},
			"true": true,
			"false": false,
			"nested": map[string]interface{}{
				"foo": "bar",
			},
		})
	}

	td.InsertRow("33333", map[string]interface{}{
		"foo": "bar",
		"array": []string{"foo", "bar", "dusty", "the aussie", "robin", "jacqueline", "charlie"},
		"true": true,
		"false": false,
		"nested": map[string]interface{}{
			"foo": "bar",
		},
	})

	assert.True(t, td.ShouldFlush(ctx))
}
