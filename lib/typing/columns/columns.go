package columns

import (
	"fmt"
	"strings"
	"sync"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
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
	defaultValue any
	backfilled   bool
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

func (c *Column) RawName() string {
	return c.name
}

// Name will give you c.name
// However, if you pass in escape, we will escape if the column name is part of the reserved words from destinations.
// If so, it'll change from `start` => `"start"` as suggested by Snowflake.
func (c *Column) Name(uppercaseEscNames bool, args *sql.NameArgs) string {
	return sql.EscapeName(c.name, uppercaseEscNames, args)
}

type Columns struct {
	columns []Column
	sync.RWMutex
}

func (c *Columns) EscapeName(uppercaseEscNames bool, args *sql.NameArgs) {
	for idx := range c.columns {
		c.columns[idx].name = c.columns[idx].Name(uppercaseEscNames, args)
	}
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
func (c *Columns) GetColumnsToUpdate(uppercaseEscNames bool, args *sql.NameArgs) []string {
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

		cols = append(cols, col.Name(uppercaseEscNames, args))
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

// UpdateQuery takes:
// columns - list of columns to iterate
// columnsToTypes - given that list, provide the types (separate list because this list may contain invalid columns
// bigQueryTypeCasting - We'll need to escape the column comparison if the column's a struct.
// It then returns a list of strings like: cc.first_name=c.first_name,cc.last_name=c.last_name,cc.email=c.email
func UpdateQuery(columns []string, columnsToTypes Columns, destKind constants.DestinationKind, uppercaseEscNames bool) string {
	columnsToTypes.EscapeName(uppercaseEscNames, &sql.NameArgs{
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
						fmt.Sprintf(`%s= CASE WHEN COALESCE(TO_JSON_STRING(cc.%s) != '{"key":"%s"}', true) THEN cc.%s ELSE c.%s END`,
							// col CASE when TO_JSON_STRING(cc.col) != { 'key': TOAST_UNAVAILABLE_VALUE }
							column, column, constants.ToastUnavailableValuePlaceholder,
							// cc.col ELSE c.col END
							column, column))
				} else if destKind == constants.Redshift {
					_columns = append(_columns,
						fmt.Sprintf(`%s= CASE WHEN COALESCE(cc.%s != JSON_PARSE('{"key":"%s"}'), true) THEN cc.%s ELSE c.%s END`,
							// col CASE when TO_JSON_STRING(cc.col) != { 'key': TOAST_UNAVAILABLE_VALUE }
							column, column, constants.ToastUnavailableValuePlaceholder,
							// cc.col ELSE c.col END
							column, column))
				} else if destKind == constants.MSSQL {
					// TODO: Add tests.
					// Microsoft SQL Server doesn't allow boolean expressions to be in the COALESCE statement.
					_columns = append(_columns,
						fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s, {}) != {'key': '%s'} THEN cc.%s ELSE c.%s END",
							column, column, constants.ToastUnavailableValuePlaceholder, column, column))
				} else {
					_columns = append(_columns,
						fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s != {'key': '%s'}, true) THEN cc.%s ELSE c.%s END",
							// col CASE WHEN cc.col
							column, column,
							// { 'key': TOAST_UNAVAILABLE_VALUE } THEN cc.col ELSE c.col END",
							constants.ToastUnavailableValuePlaceholder, column, column))
				}
			} else {
				if destKind == constants.MSSQL {
					// TODO: Add tests.
					_columns = append(_columns,
						fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s, '') != '%s' THEN cc.%s ELSE c.%s END",
							column, column, constants.ToastUnavailableValuePlaceholder, column, column))
				} else {
					// t.column3 = CASE WHEN t.column3 != '__debezium_unavailable_value' THEN t.column3 ELSE s.column3 END
					_columns = append(_columns,
						fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s != '%s', true) THEN cc.%s ELSE c.%s END",
							// col = CASE WHEN cc.col != TOAST_UNAVAILABLE_VALUE
							column, column, constants.ToastUnavailableValuePlaceholder,
							// THEN cc.col ELSE c.col END
							column, column))
				}
			}

		} else {
			// This is to make it look like: objCol = cc.objCol
			_columns = append(_columns, fmt.Sprintf("%s=cc.%s", column, column))
		}

	}

	return strings.Join(_columns, ",")
}
