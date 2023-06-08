package dml

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
)

func TestMergeStatement_TempTable(t *testing.T) {
	var cols typing.Columns
	cols.AddColumn(typing.NewColumn("order_id", typing.Integer))
	cols.AddColumn(typing.NewColumn("name", typing.String))
	cols.AddColumn(typing.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	mergeArg := MergeArgument{
		FqTableName:    "customers.orders",
		SubQuery:       "customers.orders_tmp",
		PrimaryKeys:    []string{"order_id"},
		Columns:        []string{"order_id", "name", constants.DeleteColumnMarker},
		ColumnsToTypes: cols,
		BigQuery:       true,
		SoftDelete:     false,
	}

	mergeSQL, err := MergeStatement(mergeArg)
	assert.NoError(t, err)

	assert.Contains(t, mergeSQL, "MERGE INTO customers.orders c using customers.orders_tmp as cc on c.order_id = cc.order_id", mergeSQL)
}

func TestMergeStatement_JSONKey(t *testing.T) {
	var cols typing.Columns
	cols.AddColumn(typing.NewColumn("order_oid", typing.Struct))
	cols.AddColumn(typing.NewColumn("name", typing.String))
	cols.AddColumn(typing.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	mergeArg := MergeArgument{
		FqTableName:    "customers.orders",
		SubQuery:       "customers.orders_tmp",
		PrimaryKeys:    []string{"order_oid"},
		Columns:        []string{"order_oid", "name", constants.DeleteColumnMarker},
		ColumnsToTypes: cols,
		BigQuery:       true,
		SoftDelete:     false,
	}

	mergeSQL, err := MergeStatement(mergeArg)
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, "MERGE INTO customers.orders c using customers.orders_tmp as cc on TO_JSON_STRING(c.order_oid) = TO_JSON_STRING(cc.order_oid)", mergeSQL)
}
