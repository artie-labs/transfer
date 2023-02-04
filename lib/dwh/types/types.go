package types

import (
	"sync"
)

type DwhToTablesConfigMap struct {
	fqNameToDwhTableConfig map[string]*DwhTableConfig
	sync.Mutex
}

func (d *DwhToTablesConfigMap) TableConfig(fqName string) *DwhTableConfig {
	if d == nil || d.fqNameToDwhTableConfig == nil {
		return nil
	}

	tableConfig, isOk := d.fqNameToDwhTableConfig[fqName]
	if !isOk {
		return nil
	}

	return tableConfig
}

func (d *DwhToTablesConfigMap) RemoveTableFromConfig(fqName string) {
	if d == nil || d.fqNameToDwhTableConfig == nil {
		return
	}

	d.Lock()
	defer d.Unlock()
	delete(d.fqNameToDwhTableConfig, fqName)
}

func (d *DwhToTablesConfigMap) AddTableToConfig(fqName string, config *DwhTableConfig) {
	if d == nil {
		return
	}

	d.Lock()
	defer d.Unlock()

	if d.fqNameToDwhTableConfig == nil {
		d.fqNameToDwhTableConfig = make(map[string]*DwhTableConfig)
	}

	d.fqNameToDwhTableConfig[fqName] = config
	return
}
