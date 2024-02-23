package sql_server

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
)

type SqlServer struct {
}

func (s *SqlServer) Label() constants.DestinationKind {
	return constants.SQLServer
}

func (s *SqlServer) Merge(tableData *optimization.TableData) error {
	return nil
}

func (s *SqlServer) Append(tableData *optimization.TableData) error {
	return nil
}

func (s *SqlServer) ToFullyQualifiedName(tableData *optimization.TableData, escape bool) string {
	return ""
}

func (s *SqlServer) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	return nil, nil
}

func (s *SqlServer) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string, additionalSettings types.AdditionalSettings) error {
	return nil
}
