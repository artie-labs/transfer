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

func (o *OptimizationTestSuite) TestNewTableData_TableName() {
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
		assert.Equal(o.T(), testCase.expectedName, td.Name(o.ctx, nil), testCase.name)
		assert.Equal(o.T(), testCase.expectedName, td.name, testCase.name)
		assert.Equal(o.T(), testCase.expectedSnowflakeFqName, td.ToFqName(ctx, constants.SnowflakeStages, true))
		assert.Equal(o.T(), testCase.expectedSnowflakeFqName, td.ToFqName(ctx, constants.Snowflake, true))
		assert.Equal(o.T(), testCase.expectedBigQueryFqName, td.ToFqName(ctx, constants.BigQuery, true))
		assert.Equal(o.T(), testCase.expectedBigQueryFqName, td.ToFqName(ctx, constants.BigQuery, true))
	}
}

func (o *OptimizationTestSuite) TestTableData_ReadOnlyInMemoryCols() {
	// Making sure the columns are actually read only.
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("name", typing.String))

	td := NewTableData(&cols, nil, kafkalib.TopicConfig{}, "foo")
	readOnlyCols := td.ReadOnlyInMemoryCols()
	readOnlyCols.AddColumn(columns.NewColumn("last_name", typing.String))

	// Check if last_name actually exists.
	_, isOk := td.ReadOnlyInMemoryCols().GetColumn("last_name")
	assert.False(o.T(), isOk)

	// Check length is 1.
	assert.Equal(o.T(), 1, len(td.ReadOnlyInMemoryCols().GetColumns()))
}

func (o *OptimizationTestSuite) TestTableData_UpdateInMemoryColumns() {
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
	assert.True(o.T(), isOk)

	extCol.KindDetails.ExtendedTimeDetails.Format = time.RFC3339Nano
	tableData.inMemoryColumns.UpdateColumn(columns.NewColumn(extCol.Name(o.ctx, nil), extCol.KindDetails))

	for name, colKindDetails := range map[string]typing.KindDetails{
		"foo":                  typing.String,
		"change_me":            typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"bar":                  typing.Boolean,
		"do_not_change_format": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	} {
		tableData.UpdateInMemoryColumnsFromDestination(o.ctx, columns.NewColumn(name, colKindDetails))
	}

	// It's saved back in the original format.
	_, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("foo")
	assert.False(o.T(), isOk)

	_, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("FOO")
	assert.True(o.T(), isOk)

	col, isOk := tableData.ReadOnlyInMemoryCols().GetColumn("CHANGE_me")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), ext.DateTime.Type, col.KindDetails.ExtendedTimeDetails.Type)

	// It went from invalid to boolean.
	col, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("bar")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), typing.Boolean, col.KindDetails)

	col, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("do_not_change_format")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), col.KindDetails.Kind, typing.ETime.Kind)
	assert.Equal(o.T(), col.KindDetails.ExtendedTimeDetails.Type, ext.DateTimeKindType, "correctly mapped type")
	assert.Equal(o.T(), col.KindDetails.ExtendedTimeDetails.Format, time.RFC3339Nano, "format has been preserved")
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
		}, false)
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
		}, false)
	}

	td.InsertRow("33333", map[string]interface{}{
		"foo":   "bar",
		"array": []string{"foo", "bar", "dusty", "the aussie", "robin", "jacqueline", "charlie"},
		"true":  true,
		"false": false,
		"nested": map[string]interface{}{
			"foo": "bar",
		},
	}, false)

	assert.True(t, td.ShouldFlush(ctx))
}

func TestTableData_InsertRowIntegrity(t *testing.T) {
	ctx := context.Background()
	ctx = config.InjectSettingsIntoContext(ctx, &config.Settings{Config: &config.Config{
		FlushSizeKb: 5,
		BufferRows:  20000,
	}})

	td := NewTableData(nil, nil, kafkalib.TopicConfig{}, "foo")
	assert.Equal(t, 0, int(td.Rows()))
	assert.False(t, td.ContainOtherOperations())

	for i := 0; i < 100; i++ {
		td.InsertRow("123", nil, true)
		assert.False(t, td.ContainOtherOperations())
	}

	for i := 0; i < 100; i++ {
		td.InsertRow("123", nil, false)
		assert.True(t, td.ContainOtherOperations())
	}
}
