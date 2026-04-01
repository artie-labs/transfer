package elasticsearch

import "github.com/artie-labs/transfer/lib/sql"

type TableIdentifier struct {
	dbName    string
	schema    string
	tableName string
}

func NewTableIdentifier(dbName, schema, tableName string) TableIdentifier {
	return TableIdentifier{
		dbName:    dbName,
		schema:    schema,
		tableName: tableName,
	}
}

func (t TableIdentifier) EscapedTable() string {
	return t.tableName
}

func (t TableIdentifier) Table() string {
	return t.tableName
}

func (t TableIdentifier) Schema() string {
	return t.schema
}

func (t TableIdentifier) WithTable(table string) sql.TableIdentifier {
	return NewTableIdentifier(t.dbName, t.schema, table)
}

func (t TableIdentifier) FullyQualifiedName() string {
	return t.tableName // Elasticsearch indices don't need fully qualified names like DB.Schema.Table
}

func (t TableIdentifier) WithTemporaryTable(temp bool) sql.TableIdentifier {
	return t
}

func (t TableIdentifier) TemporaryTable() bool {
	return false
}

func (t TableIdentifier) Name() string {
	return t.tableName
}
