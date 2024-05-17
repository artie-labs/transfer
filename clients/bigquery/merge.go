package bigquery

import (
	"fmt"
	"log/slog"

	bigqueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/kafkalib/partition"

	"cloud.google.com/go/bigquery"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
)

type Row struct {
	data map[string]bigquery.Value
}

func NewRow(data map[string]bigquery.Value) *Row {
	return &Row{
		data: data,
	}
}

func (r *Row) Save() (map[string]bigquery.Value, string, error) {
	return r.data, bigquery.NoDedupeID, nil
}

func (s *Store) Merge(tableData *optimization.TableData) error {
	var additionalEqualityStrings []string
	if tableData.TopicConfig().BigQueryPartitionSettings != nil {
		additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
		distinctDates, err := tableData.DistinctDates(tableData.TopicConfig().BigQueryPartitionSettings.PartitionField, additionalDateFmts)
		if err != nil {
			return fmt.Errorf("failed to generate distinct dates: %w", err)
		}

		mergeString, err := generateMergeString(tableData.TopicConfig().BigQueryPartitionSettings, distinctDates)
		if err != nil {
			slog.Warn("Failed to generate merge string", slog.Any("err", err))
			return err
		}

		additionalEqualityStrings = []string{mergeString}
	}

	return shared.Merge(s, tableData, types.MergeOpts{
		AdditionalEqualityStrings: additionalEqualityStrings,
		// BigQuery has DDL quotas.
		RetryColBackfill: true,
	})
}

// generateMergeString this is used as an equality string for the MERGE statement.
func generateMergeString(bqSettings *partition.BigQuerySettings, values []string) (string, error) {
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
					bigqueryDialect.BigQueryDialect{},
				),
				array.StringsJoinAddSingleQuotes(values)), nil
		}
	}

	return "", fmt.Errorf("unexpected partitionType: %s and/or partitionBy: %s", bqSettings.PartitionType, bqSettings.PartitionBy)
}
