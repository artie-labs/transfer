package columns

import (
	"fmt"
	"strings"
	"sync"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
)

// EscapeName - will lowercase columns and escape spaces.
func EscapeName(name string) string {
	_, name = stringutil.EscapeSpaces(strings.ToLower(name))
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
	defaultValue interface{}
	backfilled   bool
}

func (c *Column) ShouldSkip() bool {
	if c == nil || c.KindDetails.Kind == typing.Invalid.Kind {
		return true
	}

	return false
}

func UnescapeColumnName(escapedName string, destKind constants.DestinationKind) string {
	if destKind == constants.BigQuery {
		return strings.ReplaceAll(escapedName, "`", "")
	} else {
		// Snowflake does not return escaping.
		return escapedName
	}
}

func NewColumn(name string, kd typing.KindDetails) Column {
	return Column{
		name:        name,
		KindDetails: kd,
	}
}

func (c *Column) SetBackfilled(backfilled bool) {
	c.backfilled = backfilled
	return
}

func (c *Column) Backfilled() bool {
	return c.backfilled
}

func (c *Column) SetDefaultValue(value interface{}) {
	c.defaultValue = value
}

func (c *Column) ToLowerName() {
	c.name = strings.ToLower(c.name)
	return
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
	return c.defaultValue != nil && c.backfilled == false
}

type NameArgs struct {
	Escape   bool
	DestKind constants.DestinationKind
}

// Name will give you c.name
// However, if you pass in escape, we will escape if the column name is part of the reserved words from destinations.
// If so, it'll change from `start` => `"start"` as suggested by Snowflake.
func (c *Column) Name(args *NameArgs) string {
	var escape bool
	if args != nil {
		escape = args.Escape
	}

	if escape && array.StringContains(constants.ReservedKeywords, c.name) {
		if args != nil && args.DestKind == constants.BigQuery {
			// BigQuery needs backticks to escape.
			return fmt.Sprintf("`%s`", c.name)
		} else {
			// Snowflake uses quotes.
			return fmt.Sprintf(`"%s"`, c.name)
		}
	}

	return c.name
}

type Columns struct {
	columns []Column
	sync.RWMutex
}

func (c *Columns) EscapeName(args *NameArgs) {
	for idx := range c.columns {
		c.columns[idx].name = c.columns[idx].Name(args)
	}

	return
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
		name:        colName,
		KindDetails: typing.Invalid,
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
	return
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

// GetColumnsToUpdate will filter all the `Invalid` columns so that we do not update it.
// It also has an option to escape the returned columns or not. This is used mostly for the SQL MERGE queries.
func (c *Columns) GetColumnsToUpdate(args *NameArgs) []string {
	if c == nil {
		return []string{}
	}

	c.RLock()
	defer c.RUnlock()

	var cols []string
	for _, col := range c.columns {
		if col.KindDetails == typing.Invalid {
			continue
		}

		cols = append(cols, col.Name(args))
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
	for _, col := range c.columns {
		cols = append(cols, col)
	}

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

// ColumnsUpdateQuery takes:
// columns - list of columns to iterate
// columnsToTypes - given that list, provide the types (separate list because this list may contain invalid columns
// bigQueryTypeCasting - We'll need to escape the column comparison if the column's a struct.
// It then returns a list of strings like: cc.first_name=c.first_name,cc.last_name=c.last_name,cc.email=c.email
func ColumnsUpdateQuery(columns []string, columnsToTypes Columns, destKind constants.DestinationKind) string {
	columnsToTypes.EscapeName(&NameArgs{
		Escape:   true,
		DestKind: destKind,
	})

	var _columns []string
	for _, column := range columns {
		columnType, isOk := columnsToTypes.GetColumn(column)
		if isOk && columnType.ToastColumn {
			if columnType.KindDetails == typing.Struct {
				if destKind == constants.BigQuery {
					_columns = append(_columns,
						fmt.Sprintf(`%s= CASE WHEN TO_JSON_STRING(cc.%s) != '{"key":"%s"}' THEN cc.%s ELSE c.%s END`,
							// col CASE when TO_JSON_STRING(cc.col) != { 'key': TOAST_UNAVAILABLE_VALUE }
							column, column, constants.ToastUnavailableValuePlaceholder,
							// cc.col ELSE c.col END
							column, column))
				} else {
					_columns = append(_columns,
						fmt.Sprintf("%s= CASE WHEN cc.%s != {'key': '%s'} THEN cc.%s ELSE c.%s END",
							// col CASE WHEN cc.col
							column, column,
							// { 'key': TOAST_UNAVAILABLE_VALUE } THEN cc.col ELSE c.col END",
							constants.ToastUnavailableValuePlaceholder, column, column))
				}
			} else {
				// t.column3 = CASE WHEN t.column3 != '__debezium_unavailable_value' THEN t.column3 ELSE s.column3 END
				_columns = append(_columns,
					fmt.Sprintf("%s= CASE WHEN cc.%s != '%s' THEN cc.%s ELSE c.%s END",
						// col = CASE WHEN cc.col != TOAST_UNAVAILABLE_VALUE
						column, column, constants.ToastUnavailableValuePlaceholder,
						// THEN cc.col ELSE c.col END
						column, column))
			}

		} else {
			// This is to make it look like: objCol = cc.objCol
			_columns = append(_columns, fmt.Sprintf("%s=cc.%s", column, column))
		}

	}

	return strings.Join(_columns, ",")
}
