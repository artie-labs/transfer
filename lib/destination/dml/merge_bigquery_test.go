package dml

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func (m *MergeTestSuite) TestMergeStatement_TempTable() {
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("order_id", typing.Integer))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	mergeArg := &MergeArgument{
		FqTableName:       "customers.orders",
		SubQuery:          "customers.orders_tmp",
		PrimaryKeys:       []columns.Wrapper{columns.NewWrapper(columns.NewColumn("order_id", typing.Invalid), false, nil)},
		ColumnsToTypes:    cols,
		DestKind:          constants.BigQuery,
		SoftDelete:        false,
		UppercaseEscNames: ptr.ToBool(false),
	}

	mergeSQL, err := mergeArg.GetStatement()
	assert.NoError(m.T(), err)

	assert.Contains(m.T(), mergeSQL, "MERGE INTO customers.orders c using customers.orders_tmp as cc on c.order_id = cc.order_id", mergeSQL)
}

func (m *MergeTestSuite) TestMergeStatement_JSONKey() {
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("order_oid", typing.Struct))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	mergeArg := &MergeArgument{
		FqTableName:       "customers.orders",
		SubQuery:          "customers.orders_tmp",
		PrimaryKeys:       []columns.Wrapper{columns.NewWrapper(columns.NewColumn("order_oid", typing.Invalid), false, nil)},
		ColumnsToTypes:    cols,
		DestKind:          constants.BigQuery,
		SoftDelete:        false,
		UppercaseEscNames: ptr.ToBool(false),
	}

	mergeSQL, err := mergeArg.GetStatement()
	assert.NoError(m.T(), err)
	assert.Contains(m.T(), mergeSQL, "MERGE INTO customers.orders c using customers.orders_tmp as cc on TO_JSON_STRING(c.order_oid) = TO_JSON_STRING(cc.order_oid)", mergeSQL)
}
