package apachelivy

import (
	"fmt"
)

// SparkSQL does not support primary keys.
type Column struct {
	Name     string
	DataType string
	Comment  string
}

func (g GetSchemaResponse) BuildColumns() ([]Column, error) {
	colNameIndex := -1
	colTypeIndex := -1
	colCommentIndex := -1

	for i, field := range g.Schema.Fields {
		switch field.Name {
		case "col_name":
			colNameIndex = i
		case "data_type":
			colTypeIndex = i
		case "comment":
			colCommentIndex = i
		}
	}

	if colNameIndex == -1 || colTypeIndex == -1 || colCommentIndex == -1 {
		return nil, fmt.Errorf("col_name, data_type, or comment not found")
	}

	var cols []Column
	for _, row := range g.Data {
		cols = append(cols, Column{
			Name:     row[colNameIndex],
			DataType: row[colTypeIndex],
			Comment:  row[colCommentIndex],
		})
	}

	return cols, nil
}
