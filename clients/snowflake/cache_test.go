package snowflake

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"time"

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

	config := types.NewDwhTableConfig(map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	}, nil, false)
	config.DropDeletedColumns = true

	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(constants.Snowflake), config)

	nameCol := typing.Column{
		Name: "name",
		Kind: typing.String,
	}

	tc := s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake))

	val := tc.ShouldDeleteColumn(nameCol.Name, time.Now().Add(-1*6*time.Hour))
	assert.False(s.T(), val, "should not try to delete this column")
	assert.Equal(s.T(), len(s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ColumnsToDelete()), 1)

	// Now let's try to add this column back, it should delete it from the cache.
	tc.MutateInMemoryColumns(false, constants.Add, nameCol)
	assert.Equal(s.T(), len(s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ColumnsToDelete()), 0)
}

func (s *SnowflakeTestSuite) TestShouldDeleteColumn() {
	topicConfig := kafkalib.TopicConfig{
		Database:  "coffee_shop",
		TableName: "orders",
		Schema:    "public",
	}

	config := types.NewDwhTableConfig(map[string]typing.KindDetails{
		"id":          typing.Integer,
		"customer_id": typing.Integer,
		"price":       typing.Float,
		"name":        typing.String,
		"created_at":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	}, nil, false)
	config.DropDeletedColumns = true

	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(constants.Snowflake), config)

	nameCol := typing.Column{
		Name: "name",
		Kind: typing.String,
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

func (s *SnowflakeTestSuite) TestGetTableConfig() {
	// If the table does not exist, snowflakeTableConfig should say so.
	fqName := "customers.public.orders22"
	ctx := context.Background()

	s.fakeStore.QueryReturns(nil, fmt.Errorf("Table '%s' does not exist or not authorized", fqName))

	tableConfig, err := s.store.getTableConfig(ctx, fqName, &optimization.TableData{})
	assert.NotNil(s.T(), tableConfig, "config is nil")
	assert.NoError(s.T(), err)

	assert.True(s.T(), tableConfig.CreateTable)
	assert.Equal(s.T(), len(tableConfig.Columns()), 0)
	assert.False(s.T(), tableConfig.DropDeletedColumns)
}
