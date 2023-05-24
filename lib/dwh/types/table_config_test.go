package types

import (
	"sync"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestDwhTableConfig_ShouldDeleteColumn(t *testing.T) {
	dwhTableConfig := NewDwhTableConfig(&typing.Columns{}, nil, false, false)
	results := dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC())
	assert.False(t, results)
	assert.Equal(t, len(dwhTableConfig.ReadOnlyColumnsToDelete()), 0)

	// Once the flag is turned on.
	dwhTableConfig.dropDeletedColumns = true
	results = dwhTableConfig.ShouldDeleteColumn("hello", time.Now().UTC())
	assert.False(t, results)
	assert.Equal(t, len(dwhTableConfig.ReadOnlyColumnsToDelete()), 1)
}

// TestDwhTableConfig_ColumnsConcurrency this file is meant to test the concurrency methods of .Columns()
// In this test, we spin up 5 parallel Go-routines each making 100 calls to .Columns() and assert the validity of the data.
func TestDwhTableConfig_ColumnsConcurrency(t *testing.T) {
	var cols typing.Columns
	cols.AddColumn(typing.Column{
		Name:        "foo",
		KindDetails: typing.Struct,
	})
	cols.AddColumn(typing.Column{
		Name:        "bar",
		KindDetails: typing.String,
	})
	cols.AddColumn(typing.Column{
		Name:        "boolean",
		KindDetails: typing.Boolean,
	})

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
				tableCfg.Columns().UpdateColumn(typing.Column{
					Name:        "foo",
					KindDetails: kindDetails,
				})
				assert.Equal(t, 3, len(tableCfg.Columns().GetColumns()), tableCfg.Columns().GetColumns())
			}
		}(dwhTableCfg)
	}

	wg.Wait()
}

func TestDwhTableConfig_MutateInMemoryColumns(t *testing.T) {
	tc := NewDwhTableConfig(&typing.Columns{}, nil, false, false)
	for _, col := range []string{"a", "b", "c", "d", "e"} {
		tc.MutateInMemoryColumns(false, constants.Add, typing.Column{Name: col, KindDetails: typing.String})
	}

	assert.Equal(t, 5, len(tc.columns.GetColumns()))
	var wg sync.WaitGroup
	for _, addCol := range []string{"aa", "bb", "cc", "dd", "ee", "ff"} {
		wg.Add(1)
		go func(colName string) {
			defer wg.Done()
			tc.MutateInMemoryColumns(false, constants.Add, typing.Column{Name: colName, KindDetails: typing.String})
		}(addCol)
	}

	for _, removeCol := range []string{"a", "b", "c", "d", "e"} {
		wg.Add(1)
		go func(colName string) {
			defer wg.Done()
			tc.MutateInMemoryColumns(false, constants.Delete, typing.Column{Name: colName})
		}(removeCol)
	}

	wg.Wait()
	assert.Equal(t, 6, len(tc.columns.GetColumns()))
}
