package bigquery

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib/partition"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	var additionalEqualityStrings []string
	if tableData.TopicConfig().BigQueryPartitionSettings != nil {
		distinctValues, err := tableData.DistinctDates(tableData.TopicConfig().BigQueryPartitionSettings.PartitionField)
		if err != nil {
			return fmt.Errorf("failed to generate distinct dates: %w", err)
		}

		mergeString, err := generateMergeString(tableData.TopicConfig().BigQueryPartitionSettings, s.Dialect(), distinctValues)
		if err != nil {
			return fmt.Errorf("failed to generate merge string: %w", err)
		}

		additionalEqualityStrings = []string{mergeString}
	}

	return shared.Merge(ctx, s, tableData, types.MergeOpts{
		AdditionalEqualityStrings: additionalEqualityStrings,
		// BigQuery has DDL quotas.
		RetryColBackfill: true,
		// We are using BigQuery's streaming API which doesn't guarantee exactly once semantics
		SubQueryDedupe: true,
	})
}

func generateMergeString(bqSettings *partition.BigQuerySettings, dialect sql.Dialect, values []string) (string, error) {
	if err := bqSettings.Valid(); err != nil {
		return "", fmt.Errorf("failed to validate bigQuerySettings: %w", err)
	}

	if len(values) == 0 {
		return "", fmt.Errorf("values cannot be empty")
	}

	if bqSettings.PartitionType != "time" {
		return "", fmt.Errorf("unexpected partitionType: %q", bqSettings.PartitionType)
	}

	part, err := bqSettings.PartitionBy.Part()
	if err != nil {
		return "", fmt.Errorf("failed to get part: %w", err)
	}

	query := fmt.Sprintf(`EXTRACT(%s FROM %s) IN (%s)`,
		part,
		sql.QuoteTableAliasColumn(
			constants.TargetAlias,
			columns.NewColumn(bqSettings.PartitionField, typing.Invalid),
			dialect,
		),
		strings.Join(sql.QuoteLiterals(values), ","))
	return query, nil
}
