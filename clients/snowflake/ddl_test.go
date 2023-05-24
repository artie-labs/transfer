package snowflake

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SnowflakeTestSuite) TestMutateColumnsWithMemoryCacheDeletions() {
	topicConfig := kafkalib.TopicConfig{
		Database:  "coffee_shop",
		TableName: "orders",
		Schema:    "public",
	}

	var cols typing.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	} {
		cols.AddColumn(typing.Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	config := types.NewDwhTableConfig(&cols, nil, false, true)

	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(constants.Snowflake), config)

	nameCol := typing.Column{
		Name:        "name",
		KindDetails: typing.String,
	}

	tc := s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake))

	val := tc.ShouldDeleteColumn(nameCol.Name, time.Now().Add(-1*6*time.Hour))
	assert.False(s.T(), val, "should not try to delete this column")
	assert.Equal(s.T(), len(s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ReadOnlyColumnsToDelete()), 1)

	// Now let's try to add this column back, it should delete it from the cache.
	tc.MutateInMemoryColumns(false, constants.Add, nameCol)
	assert.Equal(s.T(), len(s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ReadOnlyColumnsToDelete()), 0)
}

func (s *SnowflakeTestSuite) TestShouldDeleteColumn() {
	topicConfig := kafkalib.TopicConfig{
		Database:  "coffee_shop",
		TableName: "orders",
		Schema:    "public",
	}

	var cols typing.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	} {
		cols.AddColumn(typing.Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	config := types.NewDwhTableConfig(&cols, nil, false, true)
	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(constants.Snowflake), config)

	nameCol := typing.Column{
		Name:        "name",
		KindDetails: typing.String,
	}

	// Let's try to delete name.
	allowed := s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ShouldDeleteColumn(nameCol.Name, time.Now().Add(-1*(6*time.Hour)))

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process tried to delete, but it's lagged.
	allowed = s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ShouldDeleteColumn(nameCol.Name, time.Now().Add(-1*(6*time.Hour)))

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process now caught up, and is asking if we can delete, should still be no.
	allowed = s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ShouldDeleteColumn(nameCol.Name, time.Now())
	assert.Equal(s.T(), allowed, false, "should not be allowed to delete still")

	// Process is finally ahead, has permission to delete now.
	allowed = s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ShouldDeleteColumn(nameCol.Name,
		time.Now().Add(2*constants.DeletionConfidencePadding))

	assert.Equal(s.T(), allowed, true, "should now be allowed to delete")
}

func (s *SnowflakeTestSuite) TestManipulateShouldDeleteColumn() {
	var cols typing.Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	} {
		cols.AddColumn(typing.Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
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
	s.fakeStore.QueryReturns(nil, fmt.Errorf("Table '%s' does not exist or not authorized", fqName))

	tableConfig, err := s.store.getTableConfig(s.ctx, fqName, false)
	assert.NotNil(s.T(), tableConfig, "config is nil")
	assert.NoError(s.T(), err)

	assert.True(s.T(), tableConfig.CreateTable)
	assert.Equal(s.T(), len(tableConfig.Columns().GetColumns()), 0)
	assert.False(s.T(), tableConfig.DropDeletedColumns())
}
