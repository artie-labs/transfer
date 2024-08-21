package bigquery

import (
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

func (s *Store) Merge(tableData *optimization.TableData) error {
	var additionalEqualityStrings []string
	if tableData.TopicConfig().BigQueryPartitionSettings != nil {
		distinctValues, err := tableData.DistinctTimes(
			tableData.TopicConfig().BigQueryPartitionSettings.PartitionField,
			s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats,
			tableData.TopicConfig().BigQueryPartitionSettings.PartitionBy.PartitionFormat(),
		)

		if err != nil {
			return fmt.Errorf("failed to generate distinct dates: %w", err)
		}

		mergeString, err := generateMergeString(tableData.TopicConfig().BigQueryPartitionSettings, s.Dialect(), distinctValues)
		if err != nil {
			return fmt.Errorf("failed to generate merge string: %w", err)
		}

		additionalEqualityStrings = []string{mergeString}
	}

	return shared.Merge(s, tableData, types.MergeOpts{
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

	switch bqSettings.PartitionType {
	case "time":
		switch bqSettings.PartitionBy {
		case partition.Hourly:
			return fmt.Sprintf(`EXTRACT(HOUR FROM %s) IN (%s)`,
				sql.QuoteTableAliasColumn(
					constants.TargetAlias,
					columns.NewColumn(bqSettings.PartitionField, typing.Invalid),
					dialect,
				),
				strings.Join(sql.QuoteLiterals(values), ",")), nil
		case partition.Daily:
			return fmt.Sprintf(`EXTRACT(DAY FROM %s) IN (%s)`,
				sql.QuoteTableAliasColumn(
					constants.TargetAlias,
					columns.NewColumn(bqSettings.PartitionField, typing.Invalid),
					dialect,
				),
				strings.Join(sql.QuoteLiterals(values), ",")), nil
		case partition.Monthly:
			return fmt.Sprintf(`EXTRACT(MONTH FROM %s) IN (%s)`,
				sql.QuoteTableAliasColumn(
					constants.TargetAlias,
					columns.NewColumn(bqSettings.PartitionField, typing.Invalid),
					dialect,
				),
				strings.Join(sql.QuoteLiterals(values), ",")), nil
		case partition.Yearly:
			return fmt.Sprintf(`EXTRACT(YEAR FROM %s) IN (%s)`,
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
