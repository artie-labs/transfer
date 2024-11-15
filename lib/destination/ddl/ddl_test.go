package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	bqDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/clients/redshift/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestBuildCreateTableSQL(t *testing.T) {
	{
		// No columns provided
		_, err := BuildCreateTableSQL(nil, nil, false, config.Replication, []columns.Column{})
		assert.ErrorContains(t, err, "no columns provided")
	}
	{
		// Valid
		{
			// Redshift
			{
				// No primary key
				sql, err := BuildCreateTableSQL(dialect.RedshiftDialect{}, dialect.NewTableIdentifier("schema", "table"), false, config.Replication, []columns.Column{
					columns.NewColumn("foo", typing.String),
					columns.NewColumn("bar", typing.String),
				})
				assert.NoError(t, err)
				assert.Equal(t, `CREATE TABLE IF NOT EXISTS schema."table" ("foo" VARCHAR(MAX),"bar" VARCHAR(MAX));`, sql)
			}
			{
				// With primary key
				pk := columns.NewColumn("pk", typing.String)
				pk.SetPrimaryKeyForTest(true)
				sql, err := BuildCreateTableSQL(dialect.RedshiftDialect{}, dialect.NewTableIdentifier("schema", "table"), false, config.Replication, []columns.Column{
					pk,
					columns.NewColumn("bar", typing.String),
				})
				assert.NoError(t, err)
				assert.Equal(t, `CREATE TABLE IF NOT EXISTS schema."table" ("pk" VARCHAR(MAX),"bar" VARCHAR(MAX),PRIMARY KEY ("pk"));`, sql)
			}
			{
				// With more than one primary key
				pk1 := columns.NewColumn("pk1", typing.String)
				pk1.SetPrimaryKeyForTest(true)
				pk2 := columns.NewColumn("pk2", typing.String)
				pk2.SetPrimaryKeyForTest(true)

				sql, err := BuildCreateTableSQL(dialect.RedshiftDialect{}, dialect.NewTableIdentifier("schema", "table"), false, config.Replication, []columns.Column{
					pk1,
					pk2,
					columns.NewColumn("bar", typing.String),
				})
				assert.NoError(t, err)
				assert.Equal(t, `CREATE TABLE IF NOT EXISTS schema."table" ("pk1" VARCHAR(MAX),"pk2" VARCHAR(MAX),"bar" VARCHAR(MAX),PRIMARY KEY ("pk1", "pk2"));`, sql)
			}
		}
		{
			//  BigQuery
			{
				// With primary key
				pk := columns.NewColumn("pk", typing.String)
				pk.SetPrimaryKeyForTest(true)
				sql, err := BuildCreateTableSQL(bqDialect.BigQueryDialect{}, bqDialect.NewTableIdentifier("projectID", "dataset", "table"), false, config.Replication, []columns.Column{
					pk,
					columns.NewColumn("bar", typing.String),
				})
				assert.NoError(t, err)
				assert.Equal(t, "CREATE TABLE IF NOT EXISTS `projectID`.`dataset`.`table` (`pk` string,`bar` string,PRIMARY KEY (`pk`) NOT ENFORCED)", sql)
			}
		}
	}
}

func TestBuildAlterTableAddColumns(t *testing.T) {
	{
		// No columns
		sqlParts := BuildAlterTableAddColumns(nil, nil, []columns.Column{})
		assert.Empty(t, sqlParts)
	}
	{
		// One column to add
		sqlParts := BuildAlterTableAddColumns(dialect.RedshiftDialect{}, dialect.NewTableIdentifier("schema", "table"), []columns.Column{columns.NewColumn("dusty", typing.String)})
		assert.Len(t, sqlParts, 1)
		assert.Equal(t, `ALTER TABLE schema."table" add COLUMN "dusty" VARCHAR(MAX)`, sqlParts[0])
	}
	{
		// Two columns, it skips the invalid column
		sqlParts := BuildAlterTableAddColumns(dialect.RedshiftDialect{}, dialect.NewTableIdentifier("schema", "table"),
			[]columns.Column{
				columns.NewColumn("dusty", typing.String),
				columns.NewColumn("invalid", typing.Invalid),
			},
		)
		assert.Len(t, sqlParts, 1)
		assert.Equal(t, `ALTER TABLE schema."table" add COLUMN "dusty" VARCHAR(MAX)`, sqlParts[0])
	}
	{
		// Three columns to add
		sqlParts := BuildAlterTableAddColumns(dialect.RedshiftDialect{}, dialect.NewTableIdentifier("schema", "table"),
			[]columns.Column{
				columns.NewColumn("aussie", typing.String),
				columns.NewColumn("doge", typing.String),
				columns.NewColumn("age", typing.Integer),
			},
		)

		assert.Len(t, sqlParts, 3)
		assert.Equal(t, `ALTER TABLE schema."table" add COLUMN "aussie" VARCHAR(MAX)`, sqlParts[0])
		assert.Equal(t, `ALTER TABLE schema."table" add COLUMN "doge" VARCHAR(MAX)`, sqlParts[1])
		assert.Equal(t, `ALTER TABLE schema."table" add COLUMN "age" INT8`, sqlParts[2])
	}
}

func TestAlterTableArgs_Validate(t *testing.T) {
	{
		// Invalid
		a := AlterTableArgs{
			ColumnOp: constants.Delete,
			Mode:     config.Replication,
		}
		{
			// Dialect isn't specified
			assert.ErrorContains(t, a.Validate(), "dialect cannot be nil")
		}
	}
	{
		// Valid
		a := AlterTableArgs{
			ColumnOp: constants.Add,
			Mode:     config.Replication,
			Dialect:  bqDialect.BigQueryDialect{},
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
