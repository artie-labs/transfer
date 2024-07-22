package shared

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestGetTableConfig(t *testing.T) {
	// Return early because table is found in configMap.
	cols := &columns.Columns{}
	for i := range 100 {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col-%v", i), typing.Invalid))
	}

	dwhTableCfg := types.NewDwhTableConfig(cols, nil, false, false)
	cm := &types.DwhToTablesConfigMap{}
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("dusty_the_mini_aussie")
	cm.AddTableToConfig(fakeTableID, dwhTableCfg)

	actualTableCfg, err := GetTableCfgArgs{
		Dwh:       &mocks.FakeDataWarehouse{},
		TableID:   fakeTableID,
		ConfigMap: cm,
	}.GetTableConfig()

	assert.NoError(t, err)
	assert.Equal(t, dwhTableCfg, actualTableCfg)
}
