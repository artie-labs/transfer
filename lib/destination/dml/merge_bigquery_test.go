package dml

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestMergeStatement_TempTable(t *testing.T) {
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("order_id", typing.Integer))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	mergeArg := &MergeArgument{
		TableID:           MockTableIdentifier{"customers.orders"},
		SubQuery:          "customers.orders_tmp",
		PrimaryKeys:       []columns.Wrapper{columns.NewWrapper(columns.NewColumn("order_id", typing.Invalid), false, constants.BigQuery)},
		Columns:           &cols,
		DestKind:          constants.BigQuery,
		Dialect:           sql.BigQueryDialect{},
		SoftDelete:        false,
		UppercaseEscNames: ptr.ToBool(false),
	}

	mergeSQL, err := mergeArg.GetStatement()
	assert.NoError(t, err)

	assert.Contains(t, mergeSQL, "MERGE INTO customers.orders c USING customers.orders_tmp AS cc ON c.`order_id` = cc.`order_id`", mergeSQL)
}

func TestMergeStatement_JSONKey(t *testing.T) {
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("order_oid", typing.Struct))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	mergeArg := &MergeArgument{
		TableID:           MockTableIdentifier{"customers.orders"},
		SubQuery:          "customers.orders_tmp",
		PrimaryKeys:       []columns.Wrapper{columns.NewWrapper(columns.NewColumn("order_oid", typing.Invalid), false, constants.BigQuery)},
		Columns:           &cols,
		DestKind:          constants.BigQuery,
		Dialect:           sql.BigQueryDialect{},
		SoftDelete:        false,
		UppercaseEscNames: ptr.ToBool(false),
	}

	mergeSQL, err := mergeArg.GetStatement()
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, "MERGE INTO customers.orders c USING customers.orders_tmp AS cc ON TO_JSON_STRING(c.`order_oid`) = TO_JSON_STRING(cc.`order_oid`)", mergeSQL)
}
