package snowflake

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SnowflakeTestSuite) TestMutateColumnsWithMemoryCacheDeletions() {
	fqName := "coffee_shop.public.orders"

	var cols columns.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	} {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	config := types.NewDwhTableConfig(&cols, nil, false, true)

	s.stageStore.configMap.AddTableToConfig(fqName, config)

	nameCol := columns.NewColumn("name", typing.String)
	tc := s.stageStore.configMap.TableConfig(fqName)

	val := tc.ShouldDeleteColumn(nameCol.Name(nil), time.Now().Add(-1*6*time.Hour))
	assert.False(s.T(), val, "should not try to delete this column")
	assert.Equal(s.T(), len(s.stageStore.configMap.TableConfig(fqName).ReadOnlyColumnsToDelete()), 1)

	// Now let's try to add this column back, it should delete it from the cache.
	tc.MutateInMemoryColumns(false, constants.Add, nameCol)
	assert.Equal(s.T(), len(s.stageStore.configMap.TableConfig(fqName).ReadOnlyColumnsToDelete()), 0)
}

func (s *SnowflakeTestSuite) TestShouldDeleteColumn() {
	fqName := "coffee_shop.orders.public"

	var cols columns.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	} {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	config := types.NewDwhTableConfig(&cols, nil, false, true)
	s.stageStore.configMap.AddTableToConfig(fqName, config)

	nameCol := columns.NewColumn("name", typing.String)
	// Let's try to delete name.
	allowed := s.stageStore.configMap.TableConfig(fqName).ShouldDeleteColumn(nameCol.Name(nil), time.Now().Add(-1*(6*time.Hour)))

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process tried to delete, but it's lagged.
	allowed = s.stageStore.configMap.TableConfig(fqName).ShouldDeleteColumn(nameCol.Name(nil), time.Now().Add(-1*(6*time.Hour)))

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process now caught up, and is asking if we can delete, should still be no.
	allowed = s.stageStore.configMap.TableConfig(fqName).ShouldDeleteColumn(nameCol.Name(nil), time.Now())
	assert.Equal(s.T(), allowed, false, "should not be allowed to delete still")

	// Process is finally ahead, has permission to delete now.
	allowed = s.stageStore.configMap.TableConfig(fqName).ShouldDeleteColumn(nameCol.Name(nil),
		time.Now().Add(2*constants.DeletionConfidencePadding))

	assert.Equal(s.T(), allowed, true, "should now be allowed to delete")
}

func (s *SnowflakeTestSuite) TestManipulateShouldDeleteColumn() {
	var cols columns.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	} {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	tc := types.NewDwhTableConfig(&cols, map[string]time.Time{
		"customer_id": time.Now(),
	}, false, false)

	assert.Equal(s.T(), len(tc.ReadOnlyColumnsToDelete()), 1)
	assert.False(s.T(), tc.ShouldDeleteColumn("customer_id", time.Now().Add(24*time.Hour)))
}

func (s *SnowflakeTestSuite) TestGetTableConfig() {
	// If the table does not exist, snowflakeTableConfig should say so.
	fqName := "customers.public.orders22"
	s.fakeStageStore.QueryReturns(nil, fmt.Errorf("Table '%s' does not exist or not authorized", fqName))

	tableConfig, err := s.stageStore.getTableConfig(s.ctx, fqName, false)
	assert.NotNil(s.T(), tableConfig, "config is nil")
	assert.NoError(s.T(), err)

	assert.True(s.T(), tableConfig.CreateTable())
	assert.Equal(s.T(), len(tableConfig.Columns().GetColumns()), 0)
	assert.False(s.T(), tableConfig.DropDeletedColumns())
}
