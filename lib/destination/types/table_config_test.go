package types

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/jitter"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (t *TypesTestSuite) TestDwhTableConfig_ShouldDeleteColumn() {
	ctx := config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
	})

	// Test 3 different possibilities:
	// 1. DropDeletedColumns = false, so don't delete.
	dwhTableConfig := NewDwhTableConfig(&columns.Columns{}, nil, false, false)
	for i := 0; i < 100; i++ {
		results := dwhTableConfig.ShouldDeleteColumn(ctx, "hello", time.Now().UTC(), true)
		assert.False(t.T(), results)
		assert.Equal(t.T(), len(dwhTableConfig.ReadOnlyColumnsToDelete()), 0)
	}

	// 2. DropDeletedColumns = true and ContainsOtherOperations = false, so don't delete
	dwhTableConfig = NewDwhTableConfig(&columns.Columns{}, nil, false, true)
	for i := 0; i < 100; i++ {
		results := dwhTableConfig.ShouldDeleteColumn(ctx, "hello", time.Now().UTC(), false)
		assert.False(t.T(), results)
		assert.Equal(t.T(), len(dwhTableConfig.ReadOnlyColumnsToDelete()), 0)
	}

	// 3. DropDeletedColumns = true and ContainsOtherOperations = true, now check CDC time to delete.
	dwhTableConfig = NewDwhTableConfig(&columns.Columns{}, nil, false, true)
	for i := 0; i < 100; i++ {
		results := dwhTableConfig.ShouldDeleteColumn(ctx, "hello", time.Now().UTC(), true)
		assert.False(t.T(), results)
		assert.Equal(t.T(), len(dwhTableConfig.ReadOnlyColumnsToDelete()), 1)
	}

	assert.True(t.T(), dwhTableConfig.ShouldDeleteColumn(ctx, "hello", time.Now().UTC().Add(2*constants.DeletionConfidencePadding), true))
}

// TestDwhTableConfig_ColumnsConcurrency this file is meant to test the concurrency methods of .Columns()
// In this test, we spin up 5 parallel Go-routines each making 100 calls to .Columns() and assert the validity of the data.
func TestDwhTableConfig_ColumnsConcurrency(t *testing.T) {
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("foo", typing.Struct))
	cols.AddColumn(columns.NewColumn("bar", typing.String))
	cols.AddColumn(columns.NewColumn("boolean", typing.Boolean))

	dwhTableCfg := NewDwhTableConfig(&cols, nil, false, false)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(tableCfg *DwhTableConfig) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				assert.Equal(t, 3, len(tableCfg.Columns().GetColumns()), tableCfg.Columns().GetColumns())

				kindDetails := typing.Integer
				if (j % 2) == 0 {
					kindDetails = typing.Array
				}

				tableCfg.Columns().UpdateColumn(columns.UpdateColumnArgs{
					UpdateCol: columns.NewColumn("foo", kindDetails),
				})

				assert.Equal(t, 3, len(tableCfg.Columns().GetColumns()), tableCfg.Columns().GetColumns())
			}
		}(dwhTableCfg)
	}

	wg.Wait()
}

func (t *TypesTestSuite) TestDwhTableConfig_MutateInMemoryColumns() {
	tc := NewDwhTableConfig(&columns.Columns{}, nil, false, false)
	for _, col := range []string{"a", "b", "c", "d", "e"} {
		tc.MutateInMemoryColumns(t.ctx, false, constants.Add, columns.NewColumn(col, typing.String))
	}

	assert.Equal(t.T(), 5, len(tc.columns.GetColumns()))
	var wg sync.WaitGroup
	for _, addCol := range []string{"aa", "bb", "cc", "dd", "ee", "ff"} {
		wg.Add(1)
		go func(colName string) {
			defer wg.Done()
			tc.MutateInMemoryColumns(t.ctx, false, constants.Add, columns.NewColumn(colName, typing.String))
		}(addCol)
	}

	for _, removeCol := range []string{"a", "b", "c", "d", "e"} {
		wg.Add(1)
		go func(colName string) {
			defer wg.Done()
			tc.MutateInMemoryColumns(t.ctx, false, constants.Delete, columns.NewColumn(colName, typing.Invalid))
		}(removeCol)
	}

	wg.Wait()
	assert.Equal(t.T(), 6, len(tc.columns.GetColumns()))
}

func (t *TypesTestSuite) TestDwhTableConfig_ReadOnlyColumnsToDelete() {
	colsToDelete := make(map[string]time.Time)
	for _, colToDelete := range []string{"a", "b", "c", "d"} {
		colsToDelete[colToDelete] = time.Now()
	}

	tc := NewDwhTableConfig(nil, colsToDelete, false, false)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			time.Sleep(time.Duration(jitter.JitterMs(50, 1)) * time.Millisecond)
			defer wg.Done()
			actualColsToDelete := tc.ReadOnlyColumnsToDelete()
			assert.Equal(t.T(), colsToDelete, actualColsToDelete)
		}()
	}
	wg.Wait()
}

func (t *TypesTestSuite) TestDwhTableConfig_ClearColumnsToDeleteByColName() {
	colsToDelete := make(map[string]time.Time)
	for _, colToDelete := range []string{"a", "b", "c", "d"} {
		colsToDelete[colToDelete] = time.Now()
	}

	tc := NewDwhTableConfig(nil, colsToDelete, false, false)
	var wg sync.WaitGroup
	assert.Equal(t.T(), 4, len(tc.columnsToDelete))
	for _, colToDelete := range []string{"a", "b", "c", "d"} {
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(colName string) {
				time.Sleep(time.Duration(jitter.JitterMs(50, 1)) * time.Millisecond)
				defer wg.Done()
				tc.ClearColumnsToDeleteByColName(colName)
			}(colToDelete)
		}
	}

	wg.Wait()
	assert.Equal(t.T(), 0, len(tc.columnsToDelete))
}
