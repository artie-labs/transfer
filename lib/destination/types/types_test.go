package types_test

// We are using a different pkg name because we are importing `mocks.TableIdentifier`, doing so will avoid a cyclical dependency.

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func generateDwhTableCfg() *types.DestinationTableConfig {
	var cols []columns.Column
	for _, col := range []string{"a", "b", "c", "d"} {
		cols = append(cols, columns.NewColumn(col, typing.String))
	}

	tableCfg := types.NewDestinationTableConfig(cols, false)
	colsToDelete := make(map[string]time.Time)
	for _, col := range []string{"foo", "bar", "abc", "xyz"} {
		colsToDelete[col] = time.Now()
	}

	tableCfg.SetColumnsToDelete(colsToDelete)
	return tableCfg
}

func TestDwhToTablesConfigMap_TableConfigBasic(t *testing.T) {
	dwh := &types.DestinationTableCache{}
	dwhTableConfig := generateDwhTableCfg()
	fakeTableID := &mocks.FakeTableIdentifier{}
	dwh.AddTableToConfig(fakeTableID, dwhTableConfig)
	assert.Equal(t, dwhTableConfig, dwh.GetTableConfig(fakeTableID))
}

// TestDwhToTablesConfigMap_Concurrency - has a bunch of concurrent go-routines that are rapidly adding and reading from the tableConfig.
func TestDwhToTablesConfigMap_Concurrency(t *testing.T) {
	dwh := &types.DestinationTableCache{}
	fakeTableID := &mocks.FakeTableIdentifier{}
	dwhTableCfg := generateDwhTableCfg()
	dwh.AddTableToConfig(fakeTableID, dwhTableCfg)
	var wg sync.WaitGroup
	// Write
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			dwh.AddTableToConfig(fakeTableID, dwhTableCfg)
		}
	}()

	// Read
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			assert.Equal(t, dwhTableCfg, dwh.GetTableConfig(fakeTableID))
		}
	}()

	wg.Wait()
}
