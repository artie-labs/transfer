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

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	var additionalEqualityStrings []string
	for _, predicate := range tableData.TopicConfig().MergePredicates() {
		switch predicate.PartitionType {
		case partition.TimePartitionType:
			distinctDates, err := buildDistinctDates(predicate.PartitionField, tableData.Rows())
			if err != nil {
				return false, fmt.Errorf("failed to generate distinct dates: %w", err)
			}

			mergeString, err := generateMergeString(predicate, s.Dialect(), distinctDates)
			if err != nil {
				return false, fmt.Errorf("failed to generate merge string: %w", err)
			}

			additionalEqualityStrings = []string{mergeString}
		case partition.IntegerPartitionType:
			predicates, err := shared.BuildAdditionalEqualityStrings(s.Dialect(), tableData.TopicConfig().AdditionalMergePredicates)
			if err != nil {
				return false, fmt.Errorf("failed to build additional equality strings: %w", err)
			}

			additionalEqualityStrings = append(additionalEqualityStrings, predicates...)
		default:
			return false, fmt.Errorf("unexpected partitionType: %q", predicate.PartitionType)
		}
	}

	err := shared.Merge(ctx, s, tableData, types.MergeOpts{
		AdditionalEqualityStrings: additionalEqualityStrings,
		ColumnSettings:            s.config.SharedDestinationSettings.ColumnSettings,
		// BigQuery has DDL quotas.
		RetryColBackfill: true,
	})
	if err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func generateMergeString(predicate partition.MergePredicates, dialect sql.Dialect, values []string) (string, error) {
	return fmt.Sprintf(`DATE(%s) IN (%s)`,
		sql.QuoteTableAliasColumn(
			constants.TargetAlias,
			columns.NewColumn(predicate.PartitionField, typing.Invalid),
			dialect,
		),
		strings.Join(sql.QuoteLiterals(values), ",")), nil
}
