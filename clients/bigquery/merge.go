package bigquery

import (
	"fmt"
	"log/slog"

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

		mergeString, err := tableData.TopicConfig().BigQueryPartitionSettings.GenerateMergeString(distinctDates)
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
