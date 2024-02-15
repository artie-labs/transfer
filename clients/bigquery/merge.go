package bigquery

import (
	"context"
	"fmt"
	"log/slog"

	"cloud.google.com/go/bigquery"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
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

// BackfillColumn will perform a backfill to the destination and also update the comment within a transaction.
// Source: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#column_set_options_list
func (s *Store) backfillColumn(column columns.Column, fqTableName string) error {
	if !column.ShouldBackfill() {
		// If we don't need to backfill, don't backfill.
		return nil
	}

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats

	defaultVal, err := column.DefaultValue(&columns.DefaultValueArgs{Escape: true, DestKind: s.Label()}, additionalDateFmts)
	if err != nil {
		return fmt.Errorf("failed to escape default value: %w", err)
	}

	escapedCol := column.Name(s.config.SharedDestinationConfig.UppercaseEscapedNames, &sql.NameArgs{Escape: true, DestKind: s.Label()})
	query := fmt.Sprintf(`UPDATE %s SET %s = %v WHERE %s IS NULL;`,
		// UPDATE table SET col = default_val WHERE col IS NULL
		fqTableName, escapedCol, defaultVal, escapedCol)

	slog.Info(
		"backfilling column",
		slog.String("colName", column.RawName()),
		slog.String("query", query),
		slog.String("table", fqTableName),
	)
	_, err = s.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to backfill, err: %w, query: %v", err, query)
	}

	query = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET OPTIONS (description=`%s`);",
		// ALTER TABLE table ALTER COLUMN col set OPTIONS (description=...)
		fqTableName, escapedCol, `{"backfilled": true}`,
	)
	_, err = s.Exec(query)
	return err
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	var additionalEqualityStrings []string
	if tableData.TopicConfig.BigQueryPartitionSettings != nil {
		additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
		distinctDates, err := tableData.DistinctDates(tableData.TopicConfig.BigQueryPartitionSettings.PartitionField, additionalDateFmts)
		if err != nil {
			return fmt.Errorf("failed to generate distinct dates: %w", err)
		}

		mergeString, err := tableData.TopicConfig.BigQueryPartitionSettings.GenerateMergeString(distinctDates)
		if err != nil {
			slog.Warn("Failed to generate merge string", slog.Any("err", err))
			return err
		}

		additionalEqualityStrings = []string{mergeString}
	}

	return shared.Merge(s, tableData, s.config, types.MergeOpts{
		AdditionalEqualityStrings: additionalEqualityStrings,
	})
}
