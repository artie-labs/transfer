package bigquery

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/utils"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/optimization"
)

const (
	describeNameCol    = "column_name"
	describeTypeCol    = "data_type"
	describeCommentCol = "description"
)

func (s *Store) getTableConfig(ctx context.Context, tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	return utils.GetTableConfig(ctx, utils.GetTableCfgArgs{
		Dwh:                s,
		FqName:             tableData.ToFqName(ctx, constants.BigQuery),
		ConfigMap:          s.configMap,
		Query:              fmt.Sprintf("SELECT column_name, data_type, description FROM `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name='%s';", tableData.TopicConfig.Database, tableData.Name()),
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeCommentCol,
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
	})
}
