package types

import (
	"sync"

	"github.com/artie-labs/transfer/lib/sql"
)

type DwhToTablesConfigMap struct {
	fqNameToDwhTableConfig map[string]*DwhTableConfig
	sync.RWMutex
}

func (d *DwhToTablesConfigMap) TableConfig(tableID sql.TableIdentifier) *DwhTableConfig {
	d.RLock()
	defer d.RUnlock()

	tableConfig, isOk := d.fqNameToDwhTableConfig[tableID.FullyQualifiedName()]
	if !isOk {
		return nil
	}

	return tableConfig
}

func (d *DwhToTablesConfigMap) AddTableToConfig(tableID sql.TableIdentifier, config *DwhTableConfig) {
	d.Lock()
	defer d.Unlock()

	if d.fqNameToDwhTableConfig == nil {
		d.fqNameToDwhTableConfig = make(map[string]*DwhTableConfig)
	}

	d.fqNameToDwhTableConfig[tableID.FullyQualifiedName()] = config
}

type MergeOpts struct {
	SubQueryDedupe            bool
	AdditionalEqualityStrings []string
	RetryColBackfill          bool
}

type AdditionalSettings struct {
	AdditionalCopyClause string
	DwhAppendOptions
}

type DwhAppendOptions struct {
	// ExcludeDeletedColumn - Reader uses this as part of the initial backfill. Customers that have soft deleted enabled should
	// have the column `__artie_deleted` = false.
	ShouldExcludeDeletedColumn bool
}
