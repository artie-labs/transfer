package bigquery

import (
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(tableData *optimization.TableData) error {
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	return shared.Append(s, tableData, s.config, types.AppendOpts{
		TempTableName: tableID.FullyQualifiedName(s.ShouldUppercaseEscapedNames()),
	})
}
