package types

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/jitter"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (t *TypesTestSuite) TestDwhTableConfig_ShouldDeleteColumn() {
	// Test 3 different possibilities:
	// 1. DropDeletedColumns = false, so don't delete.
	dwhTableConfig := NewDwhTableConfig(&columns.Columns{}, nil, false, false)
	for i := 0; i < 100; i++ {
		results := dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC(), true)
		assert.False(t.T(), results)
		assert.Equal(t.T(), len(dwhTableConfig.ReadOnlyColumnsToDelete()), 0)
	}

	// 2. DropDeletedColumns = true and ContainsOtherOperations = false, so don't delete
	dwhTableConfig = NewDwhTableConfig(&columns.Columns{}, nil, false, true)
	for i := 0; i < 100; i++ {
		results := dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC(), false)
		assert.False(t.T(), results)
		assert.Equal(t.T(), len(dwhTableConfig.ReadOnlyColumnsToDelete()), 0)
	}

	// 3. DropDeletedColumns = true and ContainsOtherOperations = true, now check CDC time to delete.
	dwhTableConfig = NewDwhTableConfig(&columns.Columns{}, nil, false, true)
	for i := 0; i < 100; i++ {
		results := dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC(), true)
		assert.False(t.T(), results)
		assert.Equal(t.T(), len(dwhTableConfig.ReadOnlyColumnsToDelete()), 1)
	}

	assert.True(t.T(), dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC().Add(2*constants.DeletionConfidencePadding), true))
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
				tableCfg.Columns().UpdateColumn(columns.NewColumn("foo", kindDetails))
				assert.Equal(t, 3, len(tableCfg.Columns().GetColumns()), tableCfg.Columns().GetColumns())
			}
		}(dwhTableCfg)
	}

	wg.Wait()
}

func (t *TypesTestSuite) TestDwhTableConfig_MutateInMemoryColumns() {
	tc := NewDwhTableConfig(&columns.Columns{}, nil, false, false)
	for _, col := range []string{"a", "b", "c", "d", "e"} {
		tc.MutateInMemoryColumns(false, constants.Add, columns.NewColumn(col, typing.String))
	}

	assert.Equal(t.T(), 5, len(tc.columns.GetColumns()))
	var wg sync.WaitGroup
	for _, addCol := range []string{"aa", "bb", "cc", "dd", "ee", "ff"} {
		wg.Add(1)
		go func(colName string) {
			defer wg.Done()
			tc.MutateInMemoryColumns(false, constants.Add, columns.NewColumn(colName, typing.String))
		}(addCol)
	}

	for _, removeCol := range []string{"a", "b", "c", "d", "e"} {
		wg.Add(1)
		go func(colName string) {
			defer wg.Done()
			tc.MutateInMemoryColumns(false, constants.Delete, columns.NewColumn(colName, typing.Invalid))
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
			time.Sleep(jitter.Jitter(50, 3500, 1))
			defer wg.Done()
			actualColsToDelete := tc.ReadOnlyColumnsToDelete()
			assert.Equal(t.T(), colsToDelete, actualColsToDelete)
		}()
	}
	wg.Wait()
}

func (t *TypesTestSuite) TestAuditColumnsToDelete() {
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
		colsToDelete := make(map[string]time.Time)
		for _, colToDelete := range colsToDeleteList {
			colsToDelete[colToDelete] = time.Now()
		}

		dwhTc := NewDwhTableConfig(nil, colsToDelete, false, tc.dropDeletedCols)
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

		sort.Strings(actualCols)
		assert.Equal(t.T(), tc.expectedColsRemain, actualCols, idx)
	}
}
