package types_test

import (
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestDwhTableConfig_ShouldDeleteColumn(t *testing.T) {
	// Test 3 different possibilities:
	{
		// 1. DropDeletedColumns = false, so don't delete.
		dwhTableConfig := types.NewDestinationTableConfig(nil, false)
		for i := 0; i < 100; i++ {
			results := dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC(), true)
			assert.False(t, results)
			assert.Equal(t, len(dwhTableConfig.ReadOnlyColumnsToDelete()), 0)
		}
	}
	{
		// 2. DropDeletedColumns = true and ContainsOtherOperations = false, so don't delete
		dwhTableConfig := types.NewDestinationTableConfig(nil, true)
		for i := 0; i < 100; i++ {
			results := dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC(), false)
			assert.False(t, results)
			assert.Equal(t, len(dwhTableConfig.ReadOnlyColumnsToDelete()), 0)
		}
	}
	{
		// 3. DropDeletedColumns = true and ContainsOtherOperations = true, now check CDC time to delete.
		dwhTableConfig := types.NewDestinationTableConfig(nil, true)
		for i := 0; i < 100; i++ {
			results := dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC(), true)
			assert.False(t, results)
			assert.Equal(t, len(dwhTableConfig.ReadOnlyColumnsToDelete()), 1)
		}

		assert.True(t, dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC().Add(2*constants.DeletionConfidencePadding), true))
	}
}

// TestDwhTableConfig_ColumnsConcurrency this file is meant to test the concurrency methods of .Columns()
// In this test, we spin up 5 parallel Go-routines each making 100 calls to .Columns() and assert the validity of the data.
func TestDwhTableConfig_ColumnsConcurrency(t *testing.T) {
	var cols []columns.Column
	cols = append(cols, columns.NewColumn("foo", typing.Struct))
	cols = append(cols, columns.NewColumn("bar", typing.String))
	cols = append(cols, columns.NewColumn("boolean", typing.Boolean))

	dwhTableCfg := types.NewDestinationTableConfig(cols, false)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(tableCfg *types.DestinationTableConfig) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				assert.Len(t, tableCfg.GetColumns(), 3)
				kindDetails := typing.Integer
				if (j % 2) == 0 {
					kindDetails = typing.Array
				}

				tableCfg.UpdateColumn(columns.NewColumn("foo", kindDetails))
				assert.Len(t, tableCfg.GetColumns(), 3)
			}
		}(dwhTableCfg)
	}

	wg.Wait()
}

func TestDwhTableConfig_MutateInMemoryColumns(t *testing.T) {
	tc := types.NewDestinationTableConfig(nil, false)
	for _, col := range []string{"a", "b", "c", "d", "e"} {
		tc.MutateInMemoryColumns(constants.AddColumn, columns.NewColumn(col, typing.String))
	}

	assert.Len(t, tc.GetColumns(), 5)
	var wg sync.WaitGroup
	for _, addCol := range []string{"aa", "bb", "cc", "dd", "ee", "ff"} {
		wg.Add(1)
		go func(colName string) {
			defer wg.Done()
			tc.MutateInMemoryColumns(constants.AddColumn, columns.NewColumn(colName, typing.String))
		}(addCol)
	}

	for _, removeCol := range []string{"a", "b", "c", "d", "e"} {
		wg.Add(1)
		go func(colName string) {
			defer wg.Done()
			tc.MutateInMemoryColumns(constants.DropColumn, columns.NewColumn(colName, typing.Invalid))
		}(removeCol)
	}

	wg.Wait()
	assert.Len(t, tc.GetColumns(), 6)
}

func TestDwhTableConfig_ReadOnlyColumnsToDelete(t *testing.T) {
	tc := types.NewDestinationTableConfig(nil, false)
	colsToDelete := make(map[string]time.Time)
	for _, colToDelete := range []string{"a", "b", "c", "d"} {
		colsToDelete[colToDelete] = time.Now()
	}

	tc.SetColumnsToDeleteForTest(colsToDelete)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			defer wg.Done()
			actualColsToDelete := tc.ReadOnlyColumnsToDelete()
			assert.Equal(t, colsToDelete, actualColsToDelete)
		}()
	}
	wg.Wait()
}

func TestAuditColumnsToDelete(t *testing.T) {
	type _tc struct {
		colsToDelete       []string
		dropDeletedCols    bool
		expectedColsRemain []string
	}

	colsToDeleteList := []string{"aa", "ba", "ca", "da"}
	tcs := []_tc{
		{
			colsToDelete:       []string{"aa"},
			dropDeletedCols:    false,
			expectedColsRemain: colsToDeleteList,
		},
		{
			colsToDelete:       []string{},
			dropDeletedCols:    true,
			expectedColsRemain: []string{},
		},
		{
			colsToDelete:       []string{"aa", "ba", "ccc"},
			dropDeletedCols:    true,
			expectedColsRemain: []string{"aa", "ba"},
		},
	}

	for idx, tc := range tcs {
		dwhTc := types.NewDestinationTableConfig(nil, tc.dropDeletedCols)
		colsToDelete := make(map[string]time.Time)
		for _, colToDelete := range colsToDeleteList {
			colsToDelete[colToDelete] = time.Now()
		}

		dwhTc.SetColumnsToDeleteForTest(colsToDelete)

		var cols []columns.Column
		for _, colToDelete := range tc.colsToDelete {
			cols = append(cols, columns.NewColumn(colToDelete, typing.String))
		}

		dwhTc.AuditColumnsToDelete(cols)
		var actualCols []string
		for col := range dwhTc.ReadOnlyColumnsToDelete() {
			actualCols = append(actualCols, col)
		}

		if len(actualCols) == 0 {
			actualCols = []string{}
		}

		slices.Sort(actualCols)
		assert.Equal(t, tc.expectedColsRemain, actualCols, idx)
	}
}
