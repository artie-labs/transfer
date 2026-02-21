package types

import (
	"sync"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
)

type DestinationTableConfigMap struct {
	fqNameToConfigMap map[string]*DestinationTableConfig
	sync.RWMutex
}

func (d *DestinationTableConfigMap) GetTableConfig(tableID sql.TableIdentifier) *DestinationTableConfig {
	d.RLock()
	defer d.RUnlock()

	if d.fqNameToConfigMap == nil {
		return nil
	}

	tableConfig, ok := d.fqNameToConfigMap[tableID.FullyQualifiedName()]
	if !ok {
		return nil
	}

	return tableConfig
}

func (d *DestinationTableConfigMap) RemoveTable(tableID sql.TableIdentifier) {
	d.Lock()
	defer d.Unlock()

	if d.fqNameToConfigMap != nil {
		delete(d.fqNameToConfigMap, tableID.FullyQualifiedName())
	}
}

func (d *DestinationTableConfigMap) AddTable(tableID sql.TableIdentifier, config *DestinationTableConfig) {
	d.Lock()
	defer d.Unlock()

	if d.fqNameToConfigMap == nil {
		d.fqNameToConfigMap = make(map[string]*DestinationTableConfig)
	}

	d.fqNameToConfigMap[tableID.FullyQualifiedName()] = config
}

type MergeOpts struct {
	AdditionalEqualityStrings []string
	ColumnSettings            config.SharedDestinationColumnSettings
	RetryColBackfill          bool
	// PrefixStatements are SQL statements that will be executed before the merge queries (e.g. SET @@reservation for BigQuery).
	PrefixStatements []string

	// Multi-step merge settings
	PrepareTemporaryTable              bool
	UseBuildMergeQueryIntoStagingTable bool
}

type AdditionalSettings struct {
	AdditionalCopyClause string
	ColumnSettings       config.SharedDestinationColumnSettings

	// These settings are used for the `Append` method.
	UseTempTable bool
	TempTableID  sql.TableIdentifier
}
