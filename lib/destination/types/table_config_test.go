package types_test

import (
	"math/rand"
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
