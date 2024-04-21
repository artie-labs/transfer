package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(tableData *optimization.TableData) error {
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())

	// Redshift is slightly different, we'll load and create the temporary table via shared.Append
	// Then, we'll invoke `ALTER TABLE target APPEND FROM staging` to combine the diffs.
	temporaryTableID := shared.TempTableID(tableID, tableData.TempTableSuffix())
	if err := shared.Append(s, tableData, s.config, types.AppendOpts{TempTableID: temporaryTableID}); err != nil {
		return err
	}

	_, err := s.Exec(
		fmt.Sprintf(`ALTER TABLE %s APPEND FROM %s;`, tableID.FullyQualifiedName(), temporaryTableID.FullyQualifiedName()),
	)
	return err
}

func (s *Store) Merge(tableData *optimization.TableData) error {
	return shared.Merge(s, tableData, s.config, types.MergeOpts{
		UseMergeParts: true,
		// We are adding SELECT DISTINCT here for the temporary table as an extra guardrail.
		// Redshift does not enforce any row uniqueness and there could be potential LOAD errors which will cause duplicate rows to arise.
		SubQueryDedupe: true,
	})
}
