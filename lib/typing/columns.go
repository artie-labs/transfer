package typing

type Column struct {
	Name        string
	KindDetails KindDetails
	ToastColumn bool
}

type Columns struct {
	columns []Column
}

func (c *Columns) AddColumn(col Column) {
	if col.Name == "" {
		return
	}

	if _, isOk := c.GetColumn(col.Name); isOk {
		// Column exists.
		return
	}

	c.columns = append(c.columns, col)
}

func (c *Columns) GetColumn(name string) (Column, bool) {
	for _, column := range c.columns {
		if column.Name == name {
			return column, true
		}
	}

	return Column{}, false
}

func (c *Columns) GetColumns() []Column {
	if c == nil {
		return []Column{}
	}

	return c.columns
}

func (c *Columns) UpdateColumn(updateCol Column) {
	for index, col := range c.columns {
		if col.Name == updateCol.Name {
			c.columns[index] = updateCol
			return
		}
	}
}

func (c *Columns) DeleteColumn(name string) {
	for idx, column := range c.columns {
		if column.Name == name {
			c.columns = append(c.columns[:idx], c.columns[idx+1:]...)
			return
		}
	}
}
