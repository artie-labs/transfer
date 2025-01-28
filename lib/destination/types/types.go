package types

import (
	"sync"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
)

type DwhToTablesConfigMap struct {
	fqNameToDwhTableConfig map[string]*DwhTableConfig
	sync.RWMutex
}

func (d *DwhToTablesConfigMap) TableConfigCache(tableID sql.TableIdentifier) *DwhTableConfig {
	d.RLock()
	defer d.RUnlock()

	tableConfig, isOk := d.fqNameToDwhTableConfig[tableID.FullyQualifiedName()]
	if !isOk {
		return nil
	}

	return tableConfig
}

func (d *DwhToTablesConfigMap) RemoveTableFromConfig(tableID sql.TableIdentifier) {
	d.Lock()
	defer d.Unlock()

	delete(d.fqNameToDwhTableConfig, tableID.FullyQualifiedName())
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
	AdditionalEqualityStrings []string
	ColumnSettings            config.SharedDestinationColumnSettings
	RetryColBackfill          bool
	SubQueryDedupe            bool

	// Multi-step merge settings
	UseBuildMergeQueryIntoStagingTable bool
}

type AdditionalSettings struct {
	AdditionalCopyClause string
	ColumnSettings       config.SharedDestinationColumnSettings

	// These settings are used for the `Append` method.
	UseTempTable bool
	TempTableID  sql.TableIdentifier
}
