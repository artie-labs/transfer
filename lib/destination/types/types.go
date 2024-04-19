package types

import (
	"fmt"
	"sync"
)

type DwhToTablesConfigMap struct {
	fqNameToDwhTableConfig map[string]*DwhTableConfig
	sync.RWMutex
}

func (d *DwhToTablesConfigMap) TableConfig(fqName string) *DwhTableConfig {
	d.RLock()
	defer d.RUnlock()

	tableConfig, isOk := d.fqNameToDwhTableConfig[fqName]
	if !isOk {
		return nil
	}

	return tableConfig
}

func (d *DwhToTablesConfigMap) AddTableToConfig(fqName string, config *DwhTableConfig) {
	d.Lock()
	defer d.Unlock()

	if d.fqNameToDwhTableConfig == nil {
		d.fqNameToDwhTableConfig = make(map[string]*DwhTableConfig)
	}

	d.fqNameToDwhTableConfig[fqName] = config
}

type MergeOpts struct {
	UseMergeParts             bool
	SubQueryDedupe            bool
	AdditionalEqualityStrings []string
	RetryColBackfill          bool
}

type AdditionalSettings struct {
	AdditionalCopyClause string
}

type AppendOpts struct {
	// TempTableName - sometimes the destination requires 2 steps to append to the table (e.g. Redshift), so we'll create and load the data into a staging table
	// Redshift then has a separate step after `shared.Append(...)` to merge the two tables together.
	TempTableName        string
	AdditionalCopyClause string
}

func (a AppendOpts) Validate() error {
	if a.TempTableName == "" {
		return fmt.Errorf("temp table name is required")
	}

	return nil
}

type TableIdentifier interface {
	Table() string
	WithTable(table string) TableIdentifier
	FullyQualifiedName(escape, uppercaseEscNames bool) string
}
