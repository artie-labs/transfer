package event

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

func BuildSoftPartitionSuffix(
	ctx context.Context,
	tc kafkalib.TopicConfig,
	partitionColumnValue time.Time,
	executionTime time.Time,
	tblName string,
	dest destination.Baseline,
) (string, error) {
	if !tc.SoftPartitioning.Enabled {
		return "", nil
	}
	suffix, err := tc.SoftPartitioning.PartitionFrequency.Suffix(partitionColumnValue)
	if err != nil {
		return "", fmt.Errorf("failed to get partition frequency suffix: %w for table %q schema %q", err, tc.TableName, tc.Schema)
	}
	// only works for full destinations, not just Baseline
	if destWithTableConfig, ok := dest.(destination.Destination); ok {
		// Check if we should write to compacted table
		sp := tc.SoftPartitioning
		if sp.PartitionFrequency == "" {
			return "", fmt.Errorf("partition frequency is required")
		}
		distance := sp.PartitionFrequency.PartitionDistance(partitionColumnValue, executionTime)
		if distance == 0 {
			// Same partition, use base suffix
		} else if distance < 0 {
			return "", fmt.Errorf("partition time %v for column %q is in the future of execution time %v", partitionColumnValue, sp.PartitionColumn, executionTime)
		} else {
			partitionedTableName := tblName + suffix
			tableID := dest.IdentifierFor(kafkalib.DatabaseAndSchemaPair{Database: tc.Database, Schema: tc.Schema}, partitionedTableName)
			tableConfig, err := destWithTableConfig.GetTableConfig(ctx, tableID, false)
			if err != nil {
				return "", fmt.Errorf("failed to get table config: %w", err)
			}
			// tableConfig.CreateTable() will return true if the table doesn't exist.
			if tableConfig.CreateTable() {
				suffix = kafkalib.CompactedTableSuffix
			}
		}
	}
	return suffix, nil
}
