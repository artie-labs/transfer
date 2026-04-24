package types

import (
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
)

type DestinationTableConfigMap struct {
	fqNameToConfigMap map[string]*DestinationTableConfig
	fqNameToExpiry    map[string]time.Time
	sync.RWMutex
}

func (d *DestinationTableConfigMap) GetTableConfig(tableID sql.TableIdentifier) *DestinationTableConfig {
	d.Lock()
	defer d.Unlock()

	if d.fqNameToConfigMap == nil {
		return nil
	}

	tableConfig, ok := d.fqNameToConfigMap[tableID.FullyQualifiedName()]
	if !ok {
		return nil
	}

	expiry, ok := d.fqNameToExpiry[tableID.FullyQualifiedName()]
	if !ok {
		return nil
	}
	if expiry.Before(time.Now()) {
		delete(d.fqNameToConfigMap, tableID.FullyQualifiedName())
		delete(d.fqNameToExpiry, tableID.FullyQualifiedName())
		return nil
	}

	return tableConfig
}

func (d *DestinationTableConfigMap) RemoveTable(tableID sql.TableIdentifier) {
	d.Lock()
	defer d.Unlock()

	if d.fqNameToConfigMap != nil {
		delete(d.fqNameToConfigMap, tableID.FullyQualifiedName())
		delete(d.fqNameToExpiry, tableID.FullyQualifiedName())
	}
}

func (d *DestinationTableConfigMap) AddTable(tableID sql.TableIdentifier, config *DestinationTableConfig) {
	d.Lock()
	defer d.Unlock()

	if d.fqNameToConfigMap == nil {
		d.fqNameToConfigMap = make(map[string]*DestinationTableConfig)
	}

	d.fqNameToConfigMap[tableID.FullyQualifiedName()] = config

	if d.fqNameToExpiry == nil {
		d.fqNameToExpiry = make(map[string]time.Time)
	}

	d.fqNameToExpiry[tableID.FullyQualifiedName()] = time.Now().Add(time.Hour * 24)
}

type MergeOpts struct {
	AdditionalEqualityStrings []string
	ColumnSettings            config.SharedDestinationColumnSettings
	RetryColBackfill          bool

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
