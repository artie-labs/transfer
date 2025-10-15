package columns

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
)

func EscapeName(name string) string {
	// Lowercasing and escaping spaces.
	_, name = stringutil.EscapeSpaces(strings.ToLower(name))

	// Does the column name start with a number? If so, let's prefix `col_` to the column name.
	// We're doing this most databases do not allow column names to start with a number.
	if _, err := strconv.Atoi(string(name[0])); err == nil {
		name = "col_" + name
	}

	return name
}

type Column struct {
	name        string
	primaryKey  bool
	KindDetails typing.KindDetails
	// ToastColumn indicates that the source column is a TOAST column and the value is unavailable
	// We have stripped this out.
	// Whenever we see the same column where there's an opposite value in `toastColumn`, we will trigger a flush
	ToastColumn  bool
	defaultValue any
	// TODO: Instead of using a boolean, we should be setting the value at some point.
	backfilled bool
}

func (c *Column) PrimaryKey() bool {
	return c.primaryKey
}

func (c *Column) ShouldSkip() bool {
	if c == nil || c.KindDetails.Kind == typing.Invalid.Kind {
		return true
	}

	return false
}

func NewColumn(name string, kd typing.KindDetails) Column {
	return Column{
		name:        name,
		KindDetails: kd,
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

func (c *Column) SetPrimaryKeyForTest(primaryKey bool) {
	c.primaryKey = primaryKey
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

func NewColumns(columns []Column) *Columns {
	return &Columns{
		columns: columns,
	}
}

type UpsertColumnArg struct {
	ToastCol        *bool
	PrimaryKey      *bool
	Backfilled      *bool
	StringPrecision *int32
	Place           string
}

// UpsertColumn - just a wrapper around UpdateColumn and AddColumn
// If it doesn't find a column, it'll add one where the kind = Invalid.
func (c *Columns) UpsertColumn(colName string, arg UpsertColumnArg) error {
	if colName == "" {
		return fmt.Errorf("column name is empty")
	}

	if col, ok := c.GetColumn(colName); ok {
		if arg.ToastCol != nil {
			col.ToastColumn = *arg.ToastCol
		}

		if arg.PrimaryKey != nil {
			col.primaryKey = *arg.PrimaryKey
		}

		if arg.Backfilled != nil {
			col.backfilled = *arg.Backfilled
		}

		if arg.StringPrecision != nil {
			var currentPrecision int32
			if col.KindDetails.OptionalStringPrecision != nil {
				currentPrecision = *col.KindDetails.OptionalStringPrecision
			}

			if currentPrecision > *arg.StringPrecision {
				return fmt.Errorf("cannot decrease string precision from %d to %d", currentPrecision, *arg.StringPrecision)
			}

			col.KindDetails.OptionalStringPrecision = arg.StringPrecision
		}

		c.UpdateColumn(col)
	} else {
		slog.Info("Adding a new column", slog.String("colName", colName), slog.String("place", arg.Place))
		_col := Column{
			name:        colName,
			KindDetails: typing.Invalid,
		}

		if arg.ToastCol != nil {
			_col.ToastColumn = *arg.ToastCol
		}

		if arg.PrimaryKey != nil {
			_col.primaryKey = *arg.PrimaryKey
		}

		if arg.Backfilled != nil {
			_col.backfilled = *arg.Backfilled
		}

		if arg.StringPrecision != nil {
			_col.KindDetails.OptionalStringPrecision = arg.StringPrecision
		}

		c.AddColumn(_col)
	}

	return nil
}

func (c *Columns) AddColumn(col Column) {
	if col.name == "" {
		return
	}

	if _, ok := c.GetColumn(col.name); ok {
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
		if col.KindDetails == typing.Invalid {
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
