package event

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (e *EventsTestSuite) TestBuildSoftPartitionSuffix() {
	ctx := e.T().Context()
	baseTime, err := time.Parse("2006-01-02T15:04:05Z", "2024-06-01T12:34:56Z")
	assert.NoError(e.T(), err)
	executionTime := baseTime.Add(1 * time.Hour) // 1 hour later

	e.T().Run("Soft partitioning disabled", func(t *testing.T) {
		// Soft partition disabled
		tc := kafkalib.TopicConfig{
			Database:         "customer",
			TableName:        "users",
			Schema:           "public",
			SoftPartitioning: kafkalib.SoftPartitioning{Enabled: false},
		}

		suffix, err := BuildSoftPartitionSuffix(ctx, tc, baseTime, executionTime, "users", e.fakeBaseline)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "", suffix)
	})

	e.T().Run("Soft partitioning enabled without MaxPartitions", func(t *testing.T) {
		partitionFrequencies := []kafkalib.PartitionFrequency{
			kafkalib.Monthly,
			kafkalib.Daily,
			kafkalib.Hourly,
		}

		for _, freq := range partitionFrequencies {
			tc := kafkalib.TopicConfig{
				Database:  "customer",
				TableName: "users",
				Schema:    "public",
				SoftPartitioning: kafkalib.SoftPartitioning{
					Enabled:            true,
					PartitionFrequency: freq,
					PartitionColumn:    "created_at",
					MaxPartitions:      0, // No max partitions
				},
			}

			suffix, err := BuildSoftPartitionSuffix(ctx, tc, baseTime, executionTime, "users", e.fakeBaseline)
			assert.NoError(e.T(), err)

			expectedSuffix, err := freq.Suffix(baseTime)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), expectedSuffix, suffix, "Should return base suffix for frequency %s", freq)
		}
	})

	e.T().Run("Soft partitioning with MaxPartitions and baseline destination", func(t *testing.T) {
		tc := kafkalib.TopicConfig{
			Database:  "customer",
			TableName: "users",
			Schema:    "public",
			SoftPartitioning: kafkalib.SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: kafkalib.Daily,
				PartitionColumn:    "created_at",
				MaxPartitions:      5,
			},
		}

		suffix, err := BuildSoftPartitionSuffix(ctx, tc, baseTime, executionTime, "users", e.fakeBaseline)
		assert.NoError(e.T(), err)

		expectedSuffix, err := kafkalib.Daily.Suffix(baseTime)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), expectedSuffix, suffix, "Should return base suffix when dest is baseline")
	})

	e.T().Run("Soft partitioning with MaxPartitions and full destination - existing table", func(t *testing.T) {
		tc := kafkalib.TopicConfig{
			Database:  "customer",
			TableName: "users",
			Schema:    "public",
			SoftPartitioning: kafkalib.SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: kafkalib.Daily,
				PartitionColumn:    "created_at",
				MaxPartitions:      5,
			},
		}

		// Create a mock destination that returns existing table config
		mockDest := &mocks.FakeDestination{}
		mockTableConfig := types.NewDestinationTableConfig(nil, false) // Table exists (not empty columns)
		mockDest.GetTableConfigReturns(mockTableConfig, nil)

		suffix, err := BuildSoftPartitionSuffix(ctx, tc, baseTime, executionTime, "users", mockDest)
		assert.NoError(e.T(), err)

		expectedSuffix, err := kafkalib.Daily.Suffix(baseTime)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), expectedSuffix, suffix, "Should return base suffix when table exists")
	})

	e.T().Run("Soft partitioning with MaxPartitions and full destination - new table (should compact)", func(t *testing.T) {
		tc := kafkalib.TopicConfig{
			Database:  "customer",
			TableName: "users",
			Schema:    "public",
			SoftPartitioning: kafkalib.SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: kafkalib.Daily,
				PartitionColumn:    "created_at",
				MaxPartitions:      5,
			},
		}

		// Use a time that's in the past to ensure distance > 0
		pastTime := baseTime.Add(-25 * time.Hour) // 25 hours ago, so distance > 0 for daily partitioning
		executionTime := baseTime

		// Create a mock destination that returns new table config
		mockDest := &mocks.FakeDestination{}
		mockTableConfig := types.NewDestinationTableConfig([]columns.Column{}, false) // Table doesn't exist (empty columns)
		mockDest.GetTableConfigReturns(mockTableConfig, nil)

		suffix, err := BuildSoftPartitionSuffix(ctx, tc, pastTime, executionTime, "users", mockDest)
		assert.NoError(e.T(), err)

		assert.Equal(e.T(), kafkalib.CompactedTableSuffix, suffix, "Should return compacted suffix when table should be created")
	})

	e.T().Run("Soft partitioning with MaxPartitions but distance = 0", func(t *testing.T) {
		tc := kafkalib.TopicConfig{
			Database:  "customer",
			TableName: "users",
			Schema:    "public",
			SoftPartitioning: kafkalib.SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: kafkalib.Daily,
				PartitionColumn:    "created_at",
				MaxPartitions:      5,
			},
		}

		// Use same time for partition and execution (distance = 0)
		sameTime := baseTime
		executionTime := sameTime

		mockDest := &mocks.FakeDestination{}
		mockTableConfig := types.NewDestinationTableConfig([]columns.Column{}, false) // Table doesn't exist (empty columns)
		mockDest.GetTableConfigReturns(mockTableConfig, nil)

		suffix, err := BuildSoftPartitionSuffix(ctx, tc, sameTime, executionTime, "users", mockDest)
		assert.NoError(e.T(), err)

		expectedSuffix, err := kafkalib.Daily.Suffix(sameTime)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), expectedSuffix, suffix, "Should return base suffix when distance = 0")
	})

	e.T().Run("Error cases", func(t *testing.T) {
		t.Run("Invalid partition frequency", func(t *testing.T) {
			tc := kafkalib.TopicConfig{
				Database:  "customer",
				TableName: "users",
				Schema:    "public",
				SoftPartitioning: kafkalib.SoftPartitioning{
					Enabled:            true,
					PartitionFrequency: kafkalib.PartitionFrequency("invalid"),
					PartitionColumn:    "created_at",
				},
			}

			suffix, err := BuildSoftPartitionSuffix(ctx, tc, baseTime, executionTime, "users", e.fakeBaseline)
			assert.Error(e.T(), err)
			assert.Equal(e.T(), "", suffix)
			assert.Contains(e.T(), err.Error(), "failed to get partition frequency suffix")
		})

		t.Run("Destination GetTableConfig error", func(t *testing.T) {
			tc := kafkalib.TopicConfig{
				Database:  "customer",
				TableName: "users",
				Schema:    "public",
				SoftPartitioning: kafkalib.SoftPartitioning{
					Enabled:            true,
					PartitionFrequency: kafkalib.Daily,
					PartitionColumn:    "created_at",
					MaxPartitions:      5,
				},
			}

			// Use a time that's in the past to ensure distance > 0 so we actually call GetTableConfig
			pastTime := baseTime.Add(-25 * time.Hour) // 25 hours ago, so distance > 0 for daily partitioning
			executionTime := baseTime

			mockDest := &mocks.FakeDestination{}
			mockDest.GetTableConfigReturns(nil, fmt.Errorf("database connection failed"))

			suffix, err := BuildSoftPartitionSuffix(ctx, tc, pastTime, executionTime, "users", mockDest)
			assert.Error(e.T(), err)
			assert.Equal(e.T(), "", suffix)
			assert.Contains(e.T(), err.Error(), "failed to get table config")
		})
	})
}
