package snowflake

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/utils"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/ptr"
)

func (s *Store) getTableConfig(ctx context.Context, fqName string, dropDeletedColumns bool) (*types.DwhTableConfig, error) {
	return utils.GetTableConfig(ctx, utils.GetTableCfgArgs{
		Dwh:                s,
		FqName:             fqName,
		ConfigMap:          s.configMap,
		Query:              fmt.Sprintf("DESC table %s;", fqName),
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeCommentCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: dropDeletedColumns,
	})
}
