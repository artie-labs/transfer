package snowflake

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, _ bool) error {
	// TODO: For history mode - in the future, we could also have a separate stage name for history mode so we can enable parallel processing.
	return shared.Append(ctx, s, tableData, types.AdditionalSettings{
		AdditionalCopyClause: fmt.Sprintf(`FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='%s' EMPTY_FIELD_AS_NULL=FALSE) PURGE = TRUE`, constants.NullValuePlaceholder),
	})
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	predicates, err := shared.BuildAdditionalEqualityStrings(s.Dialect(), tableData.TopicConfig().AdditionalMergePredicates)
	if err != nil {
		return false, fmt.Errorf("failed to build additional equality strings: %w", err)
	}

	mergeOpts := types.MergeOpts{AdditionalEqualityStrings: predicates}
	if tableData.MultiStepMergeSettings().Enabled {
		return shared.MultiStepMerge(ctx, s, tableData, mergeOpts)
	}

	if err := shared.Merge(ctx, s, tableData, mergeOpts); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}
