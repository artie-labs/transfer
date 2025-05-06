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
		name, ok := row[colNameIndex].(string)
		if !ok {
			return nil, fmt.Errorf("col_name is not a string, type: %T", row[colNameIndex])
		}

		dataType, ok := row[colTypeIndex].(string)
		if !ok {
			return nil, fmt.Errorf("data_type is not a string, type: %T", row[colTypeIndex])
		}

		comment, ok := row[colCommentIndex].(string)
		if !ok {
			return nil, fmt.Errorf("comment is not a string, type: %T", row[colCommentIndex])
		}

		cols = append(cols, Column{
			Name:     name,
			DataType: dataType,
			Comment:  comment,
		})
	}

	return cols, nil
}
