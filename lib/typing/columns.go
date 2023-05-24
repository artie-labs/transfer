package typing

import "sync"

type Column struct {
	Name        string
	KindDetails KindDetails
	// ToastColumn indicates that the source column is a TOAST column and the value is unavailable
	// We have stripped this out.
	// Whenever we see the same column where there's an opposite value in `toastColumn`, we will trigger a flush
	ToastColumn bool
}

type Columns struct {
	columns []Column
	sync.Mutex
}

// UpsertColumn - just a wrapper around UpdateColumn and AddColumn
// If it doesn't find a column, it'll add one where the kind = Invalid.
func (c *Columns) UpsertColumn(colName string, toastColumn bool) {
	if colName == "" {
		return
	}

	if col, isOk := c.GetColumn(colName); isOk {
		col.ToastColumn = toastColumn
		c.UpdateColumn(col)
		return
	}

	c.AddColumn(Column{
		Name:        colName,
		KindDetails: Invalid,
		ToastColumn: toastColumn,
	})
	return
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

// GetColumnsToUpdate will filter all the `Invalid` columns so that we do not update it.
func (c *Columns) GetColumnsToUpdate() []string {
	if c == nil {
		return []string{}
	}

	var cols []string
	for _, col := range c.columns {
		if col.KindDetails == Invalid {
			continue
		}

		cols = append(cols, col.Name)
	}

	return cols
}

func (c *Columns) GetColumns() []Column {
	if c == nil {
		return []Column{}
	}

	return c.columns
}

func (c *Columns) UpdateColumn(updateCol Column) {
	c.Lock()
	defer c.Unlock()

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
