package dialect

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/sql"
)

var _dialect = BigQueryDialect{}

type TableIdentifier struct {
	projectID string
	dataset   string
	table     string
}

func NewTableIdentifier(projectID, dataset, table string) TableIdentifier {
	return TableIdentifier{
		projectID: projectID,
		dataset:   dataset,
		table:     table,
	}
}

func (ti TableIdentifier) ProjectID() string {
	return ti.projectID
}

func (ti TableIdentifier) Dataset() string {
	return ti.dataset
}

func (ti TableIdentifier) EscapedTable() string {
	return _dialect.QuoteIdentifier(ti.table)
}

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) WithTable(table string) sql.TableIdentifier {
	return NewTableIdentifier(ti.projectID, ti.dataset, table)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	// The fully qualified name for BigQuery is: project_id.dataset.tableName.
	// We are escaping the project_id, dataset, and table because there could be special characters.
	return fmt.Sprintf("%s.%s.%s",
		_dialect.QuoteIdentifier(ti.projectID),
		_dialect.QuoteIdentifier(ti.dataset),
		ti.EscapedTable(),
	)
}
