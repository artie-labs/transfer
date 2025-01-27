package types

import (
	"sync"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
)

type DestinationTableCache struct {
	fqNameToDestTableCfg map[string]*DestinationTableConfig
	sync.RWMutex
}

func (d *DestinationTableCache) GetTableConfig(tableID sql.TableIdentifier) *DestinationTableConfig {
	d.RLock()
	defer d.RUnlock()

	tableConfig, isOk := d.fqNameToDestTableCfg[tableID.FullyQualifiedName()]
	if !isOk {
		return nil
	}

	return tableConfig
}

func (d *DestinationTableCache) AddTableToConfig(tableID sql.TableIdentifier, config *DestinationTableConfig) {
	d.Lock()
	defer d.Unlock()

	if d.fqNameToDestTableCfg == nil {
		d.fqNameToDestTableCfg = make(map[string]*DestinationTableConfig)
	}

	d.fqNameToDestTableCfg[tableID.FullyQualifiedName()] = config
}

type MergeOpts struct {
	AdditionalEqualityStrings []string
	ColumnSettings            config.SharedDestinationColumnSettings
	RetryColBackfill          bool
	SubQueryDedupe            bool
}

type AdditionalSettings struct {
	AdditionalCopyClause string
	ColumnSettings       config.SharedDestinationColumnSettings

	// These settings are used for the `Append` method.
	UseTempTable bool
	TempTableID  sql.TableIdentifier
}
