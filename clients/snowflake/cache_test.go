package snowflake

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"time"
)

func (s *SnowflakeTestSuite) TestMutateColumnsWithMemoryCacheDeletions() {
	topicConfig := kafkalib.TopicConfig{
		Database:  "coffee_shop",
		TableName: "orders",
		Schema:    "public",
	}

	mdConfig.snowflakeTableToConfig[topicConfig.ToFqName()] = &snowflakeTableConfig{
		Columns: map[string]typing.Kind{
			"id":          typing.Integer,
			"customer_id": typing.Integer,
			"price":       typing.Float,
			"name":        typing.String,
			"created_at":  typing.DateTime,
		},
		ColumnsToDelete: make(map[string]time.Time),
	}

	nameCol := typing.Column{
		Name: "name",
		Kind: typing.String,
	}

	val := shouldDeleteColumn(topicConfig.ToFqName(), nameCol, time.Now().Add(-1*6*time.Hour))
	assert.False(s.T(), val, "should not try to delete this column")
	assert.Equal(s.T(),
		len(mdConfig.snowflakeTableToConfig[topicConfig.ToFqName()].ColumnsToDelete), 1)

	// Now let's try to add this column back, it should delete it from the cache.
	mutateColumnsWithMemoryCache(topicConfig.ToFqName(), false, Add, nameCol)
	assert.Equal(s.T(),
		len(mdConfig.snowflakeTableToConfig[topicConfig.ToFqName()].ColumnsToDelete), 0)
}

func (s *SnowflakeTestSuite) TestShouldDeleteColumn() {
	topicConfig := kafkalib.TopicConfig{
		Database:  "coffee_shop",
		TableName: "orders",
		Schema:    "public",
	}

	mdConfig.snowflakeTableToConfig[topicConfig.ToFqName()] = &snowflakeTableConfig{
		Columns: map[string]typing.Kind{
			"id":          typing.Integer,
			"customer_id": typing.Integer,
			"price":       typing.Float,
			"name":        typing.String,
			"created_at":  typing.DateTime,
		},
		ColumnsToDelete: make(map[string]time.Time),
	}

	nameCol := typing.Column{
		Name: "name",
		Kind: typing.String,
	}

	// Let's try to delete name.
	allowed := shouldDeleteColumn(topicConfig.ToFqName(), nameCol,
		time.Now().Add(-1*(6*time.Hour)))

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process tried to delete, but it's lagged.
	allowed = shouldDeleteColumn(topicConfig.ToFqName(), nameCol,
		time.Now().Add(-1*(6*time.Hour)))

	assert.Equal(s.T(), allowed, false, "should not be allowed to delete")

	// Process now caught up, and is asking if we can delete, should still be no.
	allowed = shouldDeleteColumn(topicConfig.ToFqName(), nameCol,
		time.Now())
	assert.Equal(s.T(), allowed, false, "should not be allowed to delete still")

	// Process is finally ahead, has permission to delete now.
	allowed = shouldDeleteColumn(topicConfig.ToFqName(), nameCol,
		time.Now().Add(2*config.DeletionConfidencePadding))

	assert.Equal(s.T(), allowed, true, "should now be allowed to delete")
}

func (s *SnowflakeTestSuite) TestGetTableConfig() {
	// If the table does not exist, snowflakeTableConfig should say so.
	fqName := "customers.public.orders22"
	ctx := context.Background()

	s.fakeStore.QueryReturns(nil,
		fmt.Errorf("Table '%s' does not exist or not authorized", fqName))

	tableConfig, err := GetTableConfig(ctx, fqName)
	assert.NotNil(s.T(), tableConfig, "config is nil")
	assert.NoError(s.T(), err)

	assert.True(s.T(), tableConfig.CreateTable)
	assert.Equal(s.T(), len(tableConfig.Columns), 0)
}
