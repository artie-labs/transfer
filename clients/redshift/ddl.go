package redshift

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/clients/utils"

	"github.com/artie-labs/transfer/lib/dwh/types"
)

type getTableConfigArgs struct {
	Table              string
	Schema             string
	DropDeletedColumns bool
}

const (
	describeNameCol        = "column_name"
	describeTypeCol        = "data_type"
	describeDescriptionCol = "description"
)

func (s *Store) getTableConfig(ctx context.Context, args getTableConfigArgs) (*types.DwhTableConfig, error) {
	return utils.GetTableConfig(ctx, utils.GetTableCfgArgs{
		Dwh:       s,
		FqName:    fmt.Sprintf("%s.%s", args.Schema, args.Table),
		ConfigMap: s.configMap,
		// This query is a modified fork from: https://gist.github.com/alexanderlz/7302623
		Query: fmt.Sprintf(`select c.column_name,c.data_type,d.description 
from information_schema.columns c 
left join pg_class c1 on c.table_name=c1.relname 
left join pg_catalog.pg_namespace n on c.table_schema=n.nspname and c1.relnamespace=n.oid 
left join pg_catalog.pg_description d on d.objsubid=c.ordinal_position and d.objoid=c1.oid 
where c.table_name='%s' and c.table_schema='%s'`, args.Table, args.Schema),
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeDescriptionCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: args.DropDeletedColumns,
	})
}
