package bigquery

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/destination/types"
)

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

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) WithTable(table string) types.TableIdentifier {
	return NewTableIdentifier(ti.projectID, ti.dataset, table)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	// The fully qualified name for BigQuery is: project_id.dataset.tableName.
	// We are escaping the project_id, dataset, and table because there could be special characters.
	return fmt.Sprintf("`%s`.`%s`.`%s`", ti.projectID, ti.dataset, ti.table)
}
