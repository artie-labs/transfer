package event

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

// [BuildSoftPartitionSuffix] - This will check what the right suffix we should add to the soft-partitioned table should be.
func BuildSoftPartitionSuffix(ctx context.Context, tc kafkalib.TopicConfig, columnValue, executionTime time.Time, tblName string, dest destination.Baseline) (string, error) {
	if !tc.SoftPartitioning.Enabled {
		return "", nil
	}

	suffix, err := tc.SoftPartitioning.PartitionFrequency.Suffix(columnValue)
	if err != nil {
		return "", fmt.Errorf("failed to get partition frequency suffix: %w for table %q schema %q", err, tc.TableName, tc.Schema)
	}

	destination, ok := dest.(destination.Destination)
	if !ok {
		// Soft partitioning is only supported for [destination.Destination]
		return suffix, nil
	}

	distance := tc.SoftPartitioning.PartitionFrequency.PartitionDistance(columnValue, executionTime)
	if distance < 0 {
		return "", fmt.Errorf("partition time %v for column %q is in the future of execution time %v", columnValue, tc.SoftPartitioning.PartitionColumn, executionTime)
	} else if distance > 0 {
		partitionedTableName := tblName + suffix
		tableID := dest.IdentifierFor(kafkalib.DatabaseAndSchemaPair{Database: tc.Database, Schema: tc.Schema}, partitionedTableName)
		tableConfig, err := destination.GetTableConfig(ctx, tableID, false)
		if err != nil {
			return "", fmt.Errorf("failed to get table config: %w", err)
		}

		if tableConfig.CreateTable() {
			// If the table doesn't exist, then we should write to the compacted partition.
			return kafkalib.CompactedTableSuffix, nil
		}
	}

	return suffix, nil
}
