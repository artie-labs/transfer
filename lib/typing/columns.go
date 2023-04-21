package typing

// TODO Test this whole file.

type Column struct {
	Name        string
	KindDetails KindDetails
}

type Columns struct {
	columns []Column
}

func (c *Columns) AddColumn(col Column) {
	// TODO Test uniqueness
	c.columns = append(c.columns, col)
}

func (c *Columns) GetColumn(name string) *Column {
	for _, column := range c.columns {
		if column.Name == name {
			return &column
		}
	}

	return nil
}

func (c *Columns) GetColumns() []Column {
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
