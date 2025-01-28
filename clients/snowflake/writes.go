package snowflake

import (
	"context"

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
		AdditionalCopyClause: `FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE) PURGE = TRUE`,
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

	return shared.Merge(ctx, s, tableData, mergeOpts)
}
