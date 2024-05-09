package dml

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
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

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("customers.orders")

	mergeArg := &MergeArgument{
		TableID:     fakeTableID,
		SubQuery:    "customers.orders_tmp",
		PrimaryKeys: []columns.Column{columns.NewColumn("order_id", typing.Invalid)},
		Columns:     cols.ValidColumns(),
		Dialect:     sql.BigQueryDialect{},
		SoftDelete:  false,
	}

	mergeSQL, err := mergeArg.buildDefaultStatement()
	assert.NoError(t, err)

	assert.Contains(t, mergeSQL, "MERGE INTO customers.orders c USING customers.orders_tmp AS cc ON c.`order_id` = cc.`order_id`", mergeSQL)
}

func TestMergeStatement_JSONKey(t *testing.T) {
	orderOIDCol := columns.NewColumn("order_oid", typing.Struct)
	var cols columns.Columns
	cols.AddColumn(orderOIDCol)
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("customers.orders")

	mergeArg := &MergeArgument{
		TableID:     fakeTableID,
		SubQuery:    "customers.orders_tmp",
		PrimaryKeys: []columns.Column{orderOIDCol},
		Columns:     cols.ValidColumns(),
		Dialect:     sql.BigQueryDialect{},
		SoftDelete:  false,
	}

	mergeSQL, err := mergeArg.buildDefaultStatement()
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, "MERGE INTO customers.orders c USING customers.orders_tmp AS cc ON TO_JSON_STRING(c.`order_oid`) = TO_JSON_STRING(cc.`order_oid`)", mergeSQL)
}

func TestMergeArgument_BuildStatements_BigQuery(t *testing.T) {
	orderOIDCol := columns.NewColumn("order_oid", typing.Struct)
	var cols columns.Columns
	cols.AddColumn(orderOIDCol)
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	mergeArg := &MergeArgument{
		TableID:     &mocks.FakeTableIdentifier{},
		SubQuery:    "{SUB_QUERY}",
		PrimaryKeys: []columns.Column{orderOIDCol},
		Columns:     cols.ValidColumns(),
		Dialect:     sql.BigQueryDialect{},
		SoftDelete:  false,
	}

	mergeSQL, err := mergeArg.buildDefaultStatement()
	assert.NoError(t, err)
	statements, err := mergeArg.BuildStatements()
	assert.NoError(t, err)
	assert.Equal(t, []string{mergeSQL}, statements)
}
