package snowflake

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SnowflakeTestSuite) TestMutateColumnsWithMemoryCacheDeletions() {
	tableID := NewTableIdentifier("coffee_shop", "public", "orders")

	var cols columns.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.TimestampTZ,
	} {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	config := types.NewDwhTableConfig(&cols, nil, false, true)

	s.stageStore.configMap.AddTableToConfig(tableID, config)

	nameCol := columns.NewColumn("name", typing.String)
	tc := s.stageStore.configMap.TableConfigCache(tableID)

	val := tc.ShouldDeleteColumn(nameCol.Name(), time.Now().Add(-1*6*time.Hour), true)
	assert.False(s.T(), val, "should not try to delete this column")
	assert.Equal(s.T(), len(s.stageStore.configMap.TableConfigCache(tableID).ReadOnlyColumnsToDelete()), 1)

	// Now let's try to add this column back, it should delete it from the cache.
	tc.MutateInMemoryColumns(false, constants.Add, nameCol)
	assert.Equal(s.T(), len(s.stageStore.configMap.TableConfigCache(tableID).ReadOnlyColumnsToDelete()), 0)
}

func (s *SnowflakeTestSuite) TestShouldDeleteColumn() {
	tableID := NewTableIdentifier("coffee_shop", "orders", "public")
	var cols columns.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.TimestampTZ,
	} {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	config := types.NewDwhTableConfig(&cols, nil, false, true)
	s.stageStore.configMap.AddTableToConfig(tableID, config)

	nameCol := columns.NewColumn("name", typing.String)
	// Let's try to delete name.
	allowed := s.stageStore.configMap.TableConfigCache(tableID).ShouldDeleteColumn(nameCol.Name(),
		time.Now().Add(-1*(6*time.Hour)), true)

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process tried to delete, but it's lagged.
	allowed = s.stageStore.configMap.TableConfigCache(tableID).ShouldDeleteColumn(nameCol.Name(),
		time.Now().Add(-1*(6*time.Hour)), true)

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process now caught up, and is asking if we can delete, should still be no.
	allowed = s.stageStore.configMap.TableConfigCache(tableID).ShouldDeleteColumn(nameCol.Name(), time.Now(), true)
	assert.Equal(s.T(), allowed, false, "should not be allowed to delete still")

	// Process is finally ahead, has permission to delete now.
	allowed = s.stageStore.configMap.TableConfigCache(tableID).ShouldDeleteColumn(nameCol.Name(),
		time.Now().Add(2*constants.DeletionConfidencePadding), true)

	assert.Equal(s.T(), allowed, true, "should now be allowed to delete")
}

func (s *SnowflakeTestSuite) TestManipulateShouldDeleteColumn() {
	var cols columns.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.TimestampTZ,
	} {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	tc := types.NewDwhTableConfig(&cols, map[string]time.Time{
		"customer_id": time.Now(),
	}, false, false)

	assert.Equal(s.T(), len(tc.ReadOnlyColumnsToDelete()), 1)
	assert.False(s.T(), tc.ShouldDeleteColumn("customer_id",
		time.Now().Add(24*time.Hour), false))
}

func (s *SnowflakeTestSuite) TestGetTableConfig() {
	// If the table does not exist, snowflakeTableConfig should say so.
	fqName := "customers.public.orders22"
	s.fakeStageStore.QueryReturns(nil, fmt.Errorf("Table '%s' does not exist or not authorized", fqName))

	tableData := optimization.NewTableData(nil, config.Replication, nil,
		kafkalib.TopicConfig{Database: "customers", Schema: "public", TableName: "orders22"}, "foo")

	tableConfig, err := s.stageStore.GetTableConfig(tableData)
	assert.NotNil(s.T(), tableConfig, "config is nil")
	assert.NoError(s.T(), err)

	assert.True(s.T(), tableConfig.CreateTable())
	assert.Equal(s.T(), len(tableConfig.Columns().GetColumns()), 0)
	assert.False(s.T(), tableConfig.DropDeletedColumns())
}
