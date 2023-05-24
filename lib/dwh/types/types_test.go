package types

import (
	"sync"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
)

func generateDwhTableCfg() *DwhTableConfig {
	cols := &typing.Columns{}
	colsToDelete := make(map[string]time.Time)
	for _, col := range []string{"foo", "bar", "abc", "xyz"} {
		colsToDelete[col] = time.Now()
	}

	for _, col := range []string{"a", "b", "c", "d"} {
		cols.AddColumn(typing.Column{
			Name:        col,
			KindDetails: typing.String,
		})
	}

	return &DwhTableConfig{
		columns:         cols,
		columnsToDelete: colsToDelete,
	}
}

func TestDwhToTablesConfigMap_TableConfigBasic(t *testing.T) {
	dwh := &DwhToTablesConfigMap{}
	dwhTableConfig := generateDwhTableCfg()

	fqName := "database.schema.tableName"
	dwh.AddTableToConfig(fqName, dwhTableConfig)
	assert.Equal(t, *dwhTableConfig, *dwh.TableConfig(fqName))
}

// TestDwhToTablesConfigMap_Concurrency - has a bunch of concurrent go-routines that are rapidly adding and reading from the tableConfig.
func TestDwhToTablesConfigMap_Concurrency(t *testing.T) {
	dwh := &DwhToTablesConfigMap{}
	fqName := "db.schema.table"
	dwhTableCfg := generateDwhTableCfg()
	dwh.AddTableToConfig(fqName, dwhTableCfg)
	var wg sync.WaitGroup
	// Write
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			time.Sleep(time.Duration(jitter.JitterMs(5, 1)) * time.Millisecond)
			dwh.AddTableToConfig(fqName, dwhTableCfg)
		}
	}()

	// Read
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			time.Sleep(time.Duration(jitter.JitterMs(5, 1)) * time.Millisecond)
			assert.Equal(t, *dwhTableCfg, *dwh.TableConfig(fqName))
		}

	}()

	wg.Wait()
}
