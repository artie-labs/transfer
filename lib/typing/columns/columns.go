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
	return sql.EscapeNameIfNecessary(c.name, uppercaseEscNames, args)
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

// UpdateQuery will parse the columns and then returns a list of strings like: cc.first_name=c.first_name,cc.last_name=c.last_name,cc.email=c.email
func (c *Columns) UpdateQuery(destKind constants.DestinationKind, uppercaseEscNames bool, skipDeleteCol bool) string {
	var cols []string
	for _, column := range c.GetColumns() {
		if column.ShouldSkip() {
			continue
		}

		// skipDeleteCol is useful because we don't want to copy the deleted column over to the source table if we're doing a hard row delete.
		if skipDeleteCol && column.RawName() == constants.DeleteColumnMarker {
			continue
		}

		colName := column.Name(uppercaseEscNames, &sql.NameArgs{DestKind: destKind})
		if column.ToastColumn {
			if column.KindDetails == typing.Struct {
				cols = append(cols, processToastStructCol(colName, destKind))
			} else {
				cols = append(cols, processToastCol(colName, destKind))
			}

		} else {
			// This is to make it look like: objCol = cc.objCol
			cols = append(cols, fmt.Sprintf("%s=cc.%s", colName, colName))
		}
	}

	return strings.Join(cols, ",")
}

func processToastStructCol(colName string, destKind constants.DestinationKind) string {
	switch destKind {
	case constants.BigQuery:
		return fmt.Sprintf(`%s= CASE WHEN COALESCE(TO_JSON_STRING(cc.%s) != '{"key":"%s"}', true) THEN cc.%s ELSE c.%s END`,
			colName, colName, constants.ToastUnavailableValuePlaceholder,
			colName, colName)
	case constants.Redshift:
		return fmt.Sprintf(`%s= CASE WHEN COALESCE(cc.%s != JSON_PARSE('{"key":"%s"}'), true) THEN cc.%s ELSE c.%s END`,
			colName, colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
	case constants.MSSQL:
		// Microsoft SQL Server doesn't allow boolean expressions to be in the COALESCE statement.
		return fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s, {}) != {'key': '%s'} THEN cc.%s ELSE c.%s END",
			colName, colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
	default:
		// TODO: Change this to Snowflake and error out if the destKind isn't supported so we're explicit.
		return fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s != {'key': '%s'}, true) THEN cc.%s ELSE c.%s END",
			colName, colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
	}
}

func processToastCol(colName string, destKind constants.DestinationKind) string {
	if destKind == constants.MSSQL {
		// Microsoft SQL Server doesn't allow boolean expressions to be in the COALESCE statement.
		return fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s, '') != '%s' THEN cc.%s ELSE c.%s END", colName, colName,
			constants.ToastUnavailableValuePlaceholder, colName, colName)
	} else {
		return fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s != '%s', true) THEN cc.%s ELSE c.%s END",
			colName, colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
	}
}
