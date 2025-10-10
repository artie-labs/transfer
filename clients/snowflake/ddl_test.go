package snowflake

import (
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *SnowflakeTestSuite) TestMutateColumnsWithMemoryCacheDeletions() {
	tableID := dialect.NewTableIdentifier("coffee_shop", "public", "orders")

	var cols []columns.Column
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.TimestampTZ,
	} {
		cols = append(cols, columns.NewColumn(colName, kindDetails))
	}

	s.stageStore.configMap.AddTable(tableID, types.NewDestinationTableConfig(cols, true))
	nameCol := columns.NewColumn("name", typing.String)
	tc := s.stageStore.configMap.GetTableConfig(tableID)

	val := tc.ShouldDeleteColumn(nameCol.Name(), time.Now().Add(-1*6*time.Hour), true)
	assert.False(s.T(), val, "should not try to delete this column")
	assert.Equal(s.T(), len(s.stageStore.configMap.GetTableConfig(tableID).ReadOnlyColumnsToDelete()), 1)

	// Now let's try to add this column back, it should delete it from the cache.
	tc.MutateInMemoryColumns(constants.AddColumn, nameCol)
	assert.Equal(s.T(), len(s.stageStore.configMap.GetTableConfig(tableID).ReadOnlyColumnsToDelete()), 0)
}

func (s *SnowflakeTestSuite) TestShouldDeleteColumn() {
	tableID := dialect.NewTableIdentifier("coffee_shop", "orders", "public")
	var cols []columns.Column
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.TimestampTZ,
	} {
		cols = append(cols, columns.NewColumn(colName, kindDetails))
	}

	config := types.NewDestinationTableConfig(cols, true)
	s.stageStore.configMap.AddTable(tableID, config)

	nameCol := columns.NewColumn("name", typing.String)
	// Let's try to delete name.
	allowed := s.stageStore.configMap.GetTableConfig(tableID).ShouldDeleteColumn(nameCol.Name(),
		time.Now().Add(-1*(6*time.Hour)), true)

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process tried to delete, but it's lagged.
	allowed = s.stageStore.configMap.GetTableConfig(tableID).ShouldDeleteColumn(nameCol.Name(),
		time.Now().Add(-1*(6*time.Hour)), true)

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process now caught up, and is asking if we can delete, should still be no.
	allowed = s.stageStore.configMap.GetTableConfig(tableID).ShouldDeleteColumn(nameCol.Name(), time.Now(), true)
	assert.Equal(s.T(), allowed, false, "should not be allowed to delete still")

	// Process is finally ahead, has permission to delete now.
	allowed = s.stageStore.configMap.GetTableConfig(tableID).ShouldDeleteColumn(nameCol.Name(),
		time.Now().Add(2*constants.DeletionConfidencePadding), true)

	assert.Equal(s.T(), allowed, true, "should now be allowed to delete")
}

func (s *SnowflakeTestSuite) TestManipulateShouldDeleteColumn() {
	var cols []columns.Column
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.TimestampTZ,
	} {
		cols = append(cols, columns.NewColumn(colName, kindDetails))
	}

	tc := types.NewDestinationTableConfig(cols, false)
	tc.SetColumnsToDeleteForTest(map[string]time.Time{"customer_id": time.Now()})

	assert.Equal(s.T(), len(tc.ReadOnlyColumnsToDelete()), 1)
	assert.False(s.T(), tc.ShouldDeleteColumn("customer_id",
		time.Now().Add(24*time.Hour), false))
}
