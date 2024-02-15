package redshift

import (
	"context"

	"github.com/artie-labs/transfer/lib/destination/types"

	"github.com/artie-labs/transfer/clients/shared"

	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Merge(_ context.Context, tableData *optimization.TableData) error {
	return shared.Merge(s, tableData, s.config, types.MergeOpts{
		UseMergeParts: true,
		// We are adding SELECT DISTINCT here for the temporary table as an extra guardrail.
		// Redshift does not enforce any row uniqueness and there could be potential LOAD errors which will cause duplicate rows to arise.
		SubQueryDedupe: true,
	})
}
