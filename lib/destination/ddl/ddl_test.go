package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	bqDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestAlterTableArgs_Validate(t *testing.T) {
	{
		// Invalid
		a := AlterTableArgs{
			ColumnOp:    constants.Delete,
			CreateTable: true,
			Mode:        config.Replication,
		}
		{
			// Dialect isn't specified
			assert.ErrorContains(t, a.Validate(), "dialect cannot be nil")
		}
		{
			a.Dialect = bqDialect.BigQueryDialect{}
			assert.ErrorContains(t, a.Validate(), "incompatible operation - cannot drop columns and create table at the same time")
		}
		{
			a.CreateTable = false
			a.TemporaryTable = true
			assert.ErrorContains(t, a.Validate(), "incompatible operation - we should not be altering temporary tables, only create")
		}
	}
	{
		// Valid
		a := AlterTableArgs{
			ColumnOp:       constants.Add,
			CreateTable:    true,
			TemporaryTable: true,
			Mode:           config.Replication,
			Dialect:        bqDialect.BigQueryDialect{},
		}

		assert.NoError(t, a.Validate())
	}
}

func TestShouldCreatePrimaryKey(t *testing.T) {
	pk := columns.NewColumn("foo", typing.String)
	pk.SetPrimaryKeyForTest(true)
	{
		// Primary key check
		{
			// Column is not a primary key
			col := columns.NewColumn("foo", typing.String)
			assert.False(t, shouldCreatePrimaryKey(col, config.Replication, true))
		}
		{
			// Column is a primary key
			assert.True(t, shouldCreatePrimaryKey(pk, config.Replication, true))
		}
	}
	{
		// False because it's history mode
		// It should be false because we are appending rows to this table.
		assert.False(t, shouldCreatePrimaryKey(pk, config.History, true))
	}
	{
		// False because it's not a create table operation
		assert.False(t, shouldCreatePrimaryKey(pk, config.Replication, false))
	}
	{
		// True because it's a primary key, replication mode, and create table operation
		assert.True(t, shouldCreatePrimaryKey(pk, config.Replication, true))
	}
}
