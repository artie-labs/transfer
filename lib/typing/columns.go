package typing

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

func (c *Columns) UpdateColumn(idx int, col Column) {
	// TODO test length
	// TODO test unique
	c.columns[idx] = col
}
