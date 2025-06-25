package snowflake

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, _ bool) error {
	// TODO: For history mode - in the future, we could also have a separate stage name for history mode so we can enable parallel processing.
	return shared.Append(ctx, s, tableData, types.AdditionalSettings{
		AdditionalCopyClause: fmt.Sprintf(`FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='%s' EMPTY_FIELD_AS_NULL=FALSE) PURGE = TRUE`, constants.NullValuePlaceholder),
	})
}

func (s *Store) additionalEqualityStrings(tableData *optimization.TableData) []string {
	cols := make([]columns.Column, len(tableData.TopicConfig().AdditionalMergePredicates))
	for i, additionalMergePredicate := range tableData.TopicConfig().AdditionalMergePredicates {
		cols[i] = columns.NewColumn(additionalMergePredicate.PartitionField, typing.Invalid)
	}
	return sql.BuildColumnComparisons(cols, constants.TargetAlias, constants.StagingAlias, sql.Equal, s.Dialect())
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	mergeOpts := types.MergeOpts{
		AdditionalEqualityStrings: s.additionalEqualityStrings(tableData),
	}

	if tableData.MultiStepMergeSettings().Enabled {
		return shared.MultiStepMerge(ctx, s, tableData, mergeOpts)
	}

	if err := shared.Merge(ctx, s, tableData, mergeOpts); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func (s *Store) MergeAndAssertRows(ctx context.Context, tableData *optimization.TableData, statements []string) error {
	results, err := s.ExecContextStatements(ctx, statements)
	if err != nil {
		return fmt.Errorf("failed to execute merge statements: %w", err)
	}

	var totalRowsAffected int64
	for _, result := range results {
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		totalRowsAffected += rowsAffected
	}

	// [totalRowsAffected] may be higher if the table contains duplicate rows.
	if rows := tableData.NumberOfRows(); rows > uint(totalRowsAffected) {
		return fmt.Errorf("expected %d rows to be affected, got %d", rows, totalRowsAffected)
	}

	return nil
}
