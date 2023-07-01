package optimization

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestNewTableData_TableName(t *testing.T) {
	type _testCase struct {
		name         string
		tableName    string
		overrideName string
		schema       string
		db           string

		expectedName            string
		expectedSnowflakeFqName string
		expectedBigQueryFqName  string
		expectedRedshiftFqName  string
	}

	testCases := []_testCase{
		{
			name:      "no override is provided",
			tableName: "food",
			schema:    "public",
			db:        "db",

			expectedName:            "food",
			expectedSnowflakeFqName: "db.public.food",
			expectedBigQueryFqName:  "artie.db.food",
			expectedRedshiftFqName:  "public.food",
		},
		{
			name:         "override is provided",
			tableName:    "food",
			schema:       "public",
			overrideName: "drinks",
			db:           "db",

			expectedName:            "drinks",
			expectedSnowflakeFqName: "db.public.drinks",
			expectedBigQueryFqName:  "artie.db.drinks",
			expectedRedshiftFqName:  "public.food",
		},
	}

	ctx := config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		Config: &config.Config{
			BigQuery: &config.BigQuery{
				ProjectID: "artie",
			},
		},
	})

	for _, testCase := range testCases {
		td := NewTableData(nil, nil, kafkalib.TopicConfig{
			Database:  testCase.db,
			TableName: testCase.overrideName,
			Schema:    testCase.schema,
		}, testCase.tableName)
		assert.Equal(t, testCase.expectedName, td.Name(), testCase.name)
		assert.Equal(t, testCase.expectedName, td.name, testCase.name)
		assert.Equal(t, testCase.expectedSnowflakeFqName, td.ToFqName(ctx, constants.SnowflakeStages))
		assert.Equal(t, testCase.expectedSnowflakeFqName, td.ToFqName(ctx, constants.Snowflake))
		assert.Equal(t, testCase.expectedBigQueryFqName, td.ToFqName(ctx, constants.BigQuery))
		assert.Equal(t, testCase.expectedBigQueryFqName, td.ToFqName(ctx, constants.BigQuery))
	}
}

func TestTableData_ReadOnlyInMemoryCols(t *testing.T) {
	// Making sure the columns are actually read only.
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("name", typing.String))

	td := NewTableData(&cols, nil, kafkalib.TopicConfig{}, "foo")
	readOnlyCols := td.ReadOnlyInMemoryCols()
	readOnlyCols.AddColumn(columns.NewColumn("last_name", typing.String))

	// Check if last_name actually exists.
	_, isOk := td.ReadOnlyInMemoryCols().GetColumn("last_name")
	assert.False(t, isOk)

	// Check length is 1.
	assert.Equal(t, 1, len(td.ReadOnlyInMemoryCols().GetColumns()))
}

func TestTableData_UpdateInMemoryColumns(t *testing.T) {
	var _cols columns.Columns
	for colName, colKind := range map[string]typing.KindDetails{
		"FOO":                  typing.String,
		"bar":                  typing.Invalid,
		"CHANGE_me":            typing.String,
		"do_not_change_format": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
	} {
		_cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	tableData := &TableData{
		inMemoryColumns: &_cols,
	}

	extCol, isOk := tableData.ReadOnlyInMemoryCols().GetColumn("do_not_change_format")
	assert.True(t, isOk)

	extCol.KindDetails.ExtendedTimeDetails.Format = time.RFC3339Nano
	tableData.inMemoryColumns.UpdateColumn(columns.NewColumn(extCol.Name(nil), extCol.KindDetails))

	for name, colKindDetails := range map[string]typing.KindDetails{
		"foo":                  typing.String,
		"change_me":            typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"bar":                  typing.Boolean,
		"do_not_change_format": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	} {
		tableData.UpdateInMemoryColumnsFromDestination(columns.NewColumn(name, colKindDetails))
	}

	// It's saved back in the original format.
	_, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("foo")
	assert.False(t, isOk)

	_, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("FOO")
	assert.True(t, isOk)

	col, isOk := tableData.ReadOnlyInMemoryCols().GetColumn("CHANGE_me")
	assert.True(t, isOk)
	assert.Equal(t, ext.DateTime.Type, col.KindDetails.ExtendedTimeDetails.Type)

	// It went from invalid to boolean.
	col, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, typing.Boolean, col.KindDetails)

	col, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("do_not_change_format")
	assert.True(t, isOk)
	assert.Equal(t, col.KindDetails.Kind, typing.ETime.Kind)
	assert.Equal(t, col.KindDetails.ExtendedTimeDetails.Type, ext.DateTimeKindType, "correctly mapped type")
	assert.Equal(t, col.KindDetails.ExtendedTimeDetails.Format, time.RFC3339Nano, "format has been preserved")
}

func TestTableData_ShouldFlushRowLength(t *testing.T) {
	ctx := context.Background()
	ctx = config.InjectSettingsIntoContext(ctx, &config.Settings{Config: &config.Config{
		FlushSizeKb: 500,
		BufferRows:  2,
	}})

	// Insert 3 rows and confirm that we need to flush.
	td := NewTableData(nil, nil, kafkalib.TopicConfig{}, "foo")
	for i := 0; i < 3; i++ {
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
		FlushSizeKb: 5,
		BufferRows:  20000,
	}})

	// Insert 3 rows and confirm that we need to flush.
	td := NewTableData(nil, nil, kafkalib.TopicConfig{}, "foo")
	for i := 0; i < 45; i++ {
		assert.False(t, td.ShouldFlush(ctx))
		td.InsertRow(fmt.Sprint(i), map[string]interface{}{
			"foo":   "bar",
			"array": []string{"foo", "bar", "dusty", "the aussie", "robin", "jacqueline", "charlie"},
			"true":  true,
			"false": false,
			"nested": map[string]interface{}{
				"foo": "bar",
			},
		})
	}

	td.InsertRow("33333", map[string]interface{}{
		"foo":   "bar",
		"array": []string{"foo", "bar", "dusty", "the aussie", "robin", "jacqueline", "charlie"},
		"true":  true,
		"false": false,
		"nested": map[string]interface{}{
			"foo": "bar",
		},
	})

	assert.True(t, td.ShouldFlush(ctx))
}
