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
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client) (bool, error) {
	var additionalEqualityStrings []string
	if tableData.TopicConfig().BigQueryPartitionSettings != nil {
		distinctDates, err := buildDistinctDates(tableData.TopicConfig().BigQueryPartitionSettings.PartitionField, tableData.Rows(), s.Dialect().ReservedColumnNames())
		if err != nil {
			return false, fmt.Errorf("failed to generate distinct dates: %w", err)
		}

		if len(distinctDates) > 0 {
			mergeString, err := generateMergeString(tableData.TopicConfig().BigQueryPartitionSettings, s.Dialect(), distinctDates)
			if err != nil {
				return false, fmt.Errorf("failed to generate merge string: %w", err)
			}

			additionalEqualityStrings = []string{mergeString}
		}
	}

	if len(tableData.TopicConfig().AdditionalMergePredicates) > 0 {
		predicates, err := shared.BuildAdditionalEqualityStrings(s.Dialect(), tableData.TopicConfig().AdditionalMergePredicates)
		if err != nil {
			return false, fmt.Errorf("failed to build additional equality strings: %w", err)
		}

		additionalEqualityStrings = append(additionalEqualityStrings, predicates...)
	}

	err := shared.Merge(ctx, s, tableData, types.MergeOpts{
		AdditionalEqualityStrings: additionalEqualityStrings,
		ColumnSettings:            s.config.SharedDestinationSettings.ColumnSettings,
		// BigQuery has DDL quotas.
		RetryColBackfill: true,
		PrefixStatements: s.buildPrefixStatements(),
	}, whClient)
	if err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func generateMergeString(bqSettings *partition.BigQuerySettings, dialect sql.Dialect, values []string) (string, error) {
	if err := bqSettings.Valid(); err != nil {
		return "", fmt.Errorf("failed to validate bigQuerySettings: %w", err)
	}

	if len(values) == 0 {
		return "", fmt.Errorf("values cannot be empty")
	}

	switch bqSettings.PartitionType {
	case "time":
		switch bqSettings.PartitionBy {
		case "daily":
			return fmt.Sprintf(`DATE(%s) IN (%s)`,
				sql.QuoteTableAliasColumn(
					constants.TargetAlias,
					columns.NewColumn(bqSettings.PartitionField, typing.Invalid),
					dialect,
				),
				strings.Join(sql.QuoteLiterals(values), ",")), nil
		}
	}

	return "", fmt.Errorf("unexpected partitionType: %s and/or partitionBy: %s", bqSettings.PartitionType, bqSettings.PartitionBy)
}

func (s *Store) buildPrefixStatements() []string {
	var result []string
	if s.config.BigQuery.Reservation != "" {
		result = append(result, fmt.Sprintf("SET @@reservation = %s", sql.QuoteLiteral(s.config.BigQuery.Reservation)))
	}

	return result
}
