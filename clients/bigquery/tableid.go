package bigquery

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
)

type TableIdentifier struct {
	projectID             string
	dataset               string
	table                 string
	uppercaseEscapedNames bool
}

func NewTableIdentifier(projectID, dataset, table string, uppercaseEscapedNames bool) TableIdentifier {
	return TableIdentifier{
		projectID:             projectID,
		dataset:               dataset,
		table:                 table,
		uppercaseEscapedNames: uppercaseEscapedNames,
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
	return NewTableIdentifier(ti.projectID, ti.dataset, table, ti.uppercaseEscapedNames)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	// The fully qualified name for BigQuery is: project_id.dataset.tableName.
	// We are escaping the project_id and dataset because there could be special characters.
	return fmt.Sprintf(
		"`%s`.`%s`.%s",
		ti.projectID,
		ti.dataset,
		sql.EscapeNameIfNecessary(ti.table, ti.uppercaseEscapedNames, &sql.NameArgs{Escape: true, DestKind: constants.BigQuery}),
	)
}
