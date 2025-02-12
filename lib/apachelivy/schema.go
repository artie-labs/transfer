package apachelivy

import (
	"fmt"
)

type Column struct {
	Name     string
	DataType string
	Comment  string
}

func (g GetSchemaResponse) BuildColumns() ([]Column, error) {
	// TODO: Primary key

	colNameIndex := -1
	colTypeIndex := -1
	colCommentIndex := -1

	for i, field := range g.Schema.Fields {
		switch field.Name {
		case "col_name":
			colNameIndex = i
		case "col_type":
			colTypeIndex = i
		case "col_comment":
			colCommentIndex = i
		}
	}

	if colNameIndex == -1 || colTypeIndex == -1 || colCommentIndex == -1 {
		return nil, fmt.Errorf("col_name, col_type, or col_comment not found")
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
