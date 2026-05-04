package snowflake

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/webhooks"
)

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooks.Client, _ bool) error {
	// TODO: For history mode - in the future, we could also have a separate stage name for history mode so we can enable parallel processing.
	return shared.Append(ctx, s, tableData, whClient, types.AdditionalSettings{
		AdditionalCopyClause: fmt.Sprintf(`FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='%s' EMPTY_FIELD_AS_NULL=FALSE) PURGE = TRUE`, constants.NullValuePlaceholder),
	})
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooks.Client) (bool, error) {
	predicates, err := shared.BuildAdditionalEqualityStrings(s.Dialect(), tableData.TopicConfig().AdditionalMergePredicates)
	if err != nil {
		return false, fmt.Errorf("failed to build additional equality strings: %w", err)
	}

	tc := tableData.TopicConfig()
	if tc.EnableMergePushDownFilter && tc.IncludeArtieUpdatedAt && tableData.ContainsOnlyCreates() && tableData.MinExecutionTime() != nil {
		filter := fmt.Sprintf(`%s."%s" >= '%s'`,
			constants.TargetAlias,
			constants.UpdateColumnMarker,
			tableData.MinExecutionTime().Format(time.RFC3339Nano),
		)
		predicates = append(predicates, filter)
		slog.Info("Applying merge push-down filter", slog.String("table", tableData.Name()), slog.String("filter", filter))
	}

	mergeOpts := types.MergeOpts{AdditionalEqualityStrings: predicates}
	if tableData.MultiStepMergeSettings().Enabled {
		return shared.MultiStepMerge(ctx, s, tableData, mergeOpts, whClient)
	}

	if err := shared.Merge(ctx, s, tableData, mergeOpts, whClient); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}
