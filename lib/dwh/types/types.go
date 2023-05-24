package types

import (
	"sync"
)

type DwhToTablesConfigMap struct {
	fqNameToDwhTableConfig map[string]*DwhTableConfig
	sync.RWMutex
}

func (d *DwhToTablesConfigMap) TableConfig(fqName string) *DwhTableConfig {
	// TODO test
	d.RLock()
	defer d.RUnlock()

	tableConfig, isOk := d.fqNameToDwhTableConfig[fqName]
	if !isOk {
		return nil
	}

	return tableConfig
}

func (d *DwhToTablesConfigMap) RemoveTableFromConfig(fqName string) {
	// TODO test
	d.Lock()
	defer d.Unlock()
	delete(d.fqNameToDwhTableConfig, fqName)
}

func (d *DwhToTablesConfigMap) AddTableToConfig(fqName string, config *DwhTableConfig) {
	// TODO test
	d.Lock()
	defer d.Unlock()

	if d.fqNameToDwhTableConfig == nil {
		d.fqNameToDwhTableConfig = make(map[string]*DwhTableConfig)
	}

	d.fqNameToDwhTableConfig[fqName] = config
	return
}
