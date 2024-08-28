package columns

import (
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
)

// EscapeName - will lowercase columns and escape spaces.
func EscapeName(name string) string {
	_, name = stringutil.EscapeSpaces(strings.ToLower(name))
	return name
}

type Column struct {
	name       string
	primaryKey bool

	// TODO: Start migrating usages towards [DestKindDetails]
	// destKindDetails - This is what the column looks like in the destination
	destKindDetails *typing.KindDetails
	// SourceKindDetails - This is what the column looks like from the source
	SourceKindDetails typing.KindDetails
	// ToastColumn indicates that the source column is a TOAST column and the value is unavailable
	// We have stripped this out.
	// Whenever we see the same column where there's an opposite value in `toastColumn`, we will trigger a flush
	ToastColumn  bool
	defaultValue any
	backfilled   bool
}

func (c Column) DestKindDetails() typing.KindDetails {
	if c.destKindDetails != nil {
		return *c.destKindDetails
	}

	return c.SourceKindDetails
}

func (c *Column) PrimaryKey() bool {
	return c.primaryKey
}

func (c *Column) ShouldSkip() bool {
	if c == nil || c.SourceKindDetails.Kind == typing.Invalid.Kind {
		return true
	}

	return false
}

func NewColumn(name string, kd typing.KindDetails) Column {
	return Column{
		name:              name,
		SourceKindDetails: kd,
	}
}

// NewColumnWithDefaultValue creates a new column with a default value. Only used for testing.
func NewColumnWithDefaultValue(name string, kd typing.KindDetails, defaultValue any) Column {
	column := NewColumn(name, kd)
	column.defaultValue = defaultValue
	return column
}

func (c *Column) SetBackfilled(backfilled bool) {
	c.backfilled = backfilled
}

func (c *Column) Backfilled() bool {
	return c.backfilled
}

func (c *Column) SetDefaultValue(value any) {
	c.defaultValue = value
}

func (c *Column) ToLowerName() {
	c.name = strings.ToLower(c.name)
}

func (c *Column) ShouldBackfill() bool {
	if c.primaryKey {
		// Never need to backfill primary key.
		return false
	}

	if c.ShouldSkip() {
		// Don't backfill
		return false
	}

	// Should backfill if the default value is not null and the column has not been backfilled.
	return c.defaultValue != nil && !c.backfilled
}

func (c *Column) Name() string {
	return c.name
}

func (c *Column) DefaultValue() any {
	return c.defaultValue
}

type Columns struct {
	columns []Column
	sync.RWMutex
}

type UpsertColumnArg struct {
	ToastCol   *bool
	PrimaryKey *bool
	Backfilled *bool
}

// UpsertColumn - just a wrapper around UpdateColumn and AddColumn
// If it doesn't find a column, it'll add one where the kind = Invalid.
func (c *Columns) UpsertColumn(colName string, arg UpsertColumnArg) {
	if colName == "" {
		return
	}

	if col, isOk := c.GetColumn(colName); isOk {
		if arg.ToastCol != nil {
			col.ToastColumn = *arg.ToastCol
		}

		if arg.PrimaryKey != nil {
			col.primaryKey = *arg.PrimaryKey
		}

		if arg.Backfilled != nil {
			col.backfilled = *arg.Backfilled
		}

		c.UpdateColumn(col)
		return
	}

	col := Column{
		name:              colName,
		SourceKindDetails: typing.Invalid,
	}

	if arg.ToastCol != nil {
		col.ToastColumn = *arg.ToastCol
	}

	if arg.PrimaryKey != nil {
		col.primaryKey = *arg.PrimaryKey
	}

	if arg.Backfilled != nil {
		col.backfilled = *arg.Backfilled
	}

	c.AddColumn(col)
}

func (c *Columns) AddColumn(col Column) {
	if col.name == "" {
		return
	}

	if _, isOk := c.GetColumn(col.name); isOk {
		// Column exists.
		return
	}

	c.Lock()
	defer c.Unlock()

	c.columns = append(c.columns, col)
}

func (c *Columns) GetColumn(name string) (Column, bool) {
	c.RLock()
	defer c.RUnlock()

	for _, column := range c.columns {
		if column.name == name {
			return column, true
		}
	}

	return Column{}, false
}

// ValidColumns will filter all the `Invalid` columns so that we do not update them.
// This is used mostly for the SQL MERGE queries.
func (c *Columns) ValidColumns() []Column {
	if c == nil {
		return []Column{}
	}

	c.RLock()
	defer c.RUnlock()

	var cols []Column
	for _, col := range c.columns {
		if col.SourceKindDetails == typing.Invalid {
			continue
		}

		cols = append(cols, col)
	}
	return cols
}

func (c *Columns) GetColumns() []Column {
	if c == nil {
		return []Column{}
	}

	c.RLock()
	defer c.RUnlock()

	var cols []Column
	cols = append(cols, c.columns...)
	return cols
}

// UpdateColumn will update the column and also preserve the `defaultValue` from the previous column if the new column does not have one.
func (c *Columns) UpdateColumn(updateCol Column) {
	c.Lock()
	defer c.Unlock()

	for index, col := range c.columns {
		if col.name == updateCol.name {
			c.columns[index] = updateCol
			return
		}
	}
}

func (c *Columns) DeleteColumn(name string) {
	c.Lock()
	defer c.Unlock()

	for idx, column := range c.columns {
		if column.name == name {
			c.columns = append(c.columns[:idx], c.columns[idx+1:]...)
			return
		}
	}
}

// RemoveDeleteColumnMarker removes the deleted column marker from a slice and returns a new slice.
// If the marker wasn't present, it returns an error.
func RemoveDeleteColumnMarker(cols []Column) ([]Column, error) {
	origLength := len(cols)
	// Use [slices.Clone] because [slices.DeleteFunc] mutates its inputs.
	cols = slices.DeleteFunc(slices.Clone(cols), func(col Column) bool { return col.Name() == constants.DeleteColumnMarker })
	if len(cols) == origLength {
		return []Column{}, errors.New("artie delete flag doesn't exist")
	}

	return cols, nil
}

func RemoveOnlySetDeleteColumnMarker(cols []Column) ([]Column, error) {
	origLength := len(cols)
	// Use [slices.Clone] because [slices.DeleteFunc] mutates its inputs.
	cols = slices.DeleteFunc(slices.Clone(cols), func(col Column) bool { return col.Name() == constants.OnlySetDeleteColumnMarker })
	if len(cols) == origLength {
		return []Column{}, errors.New("artie only_set_delete flag doesn't exist")
	}

	return cols, nil
}

// ColumnNames takes a slice of [Column] and returns the names as a slice of strings.
func ColumnNames(cols []Column) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = col.Name()
	}
	return result
}
