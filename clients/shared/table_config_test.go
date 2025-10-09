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
	var cols []columns.Column
	for i := range 100 {
		cols = append(cols, columns.NewColumn(fmt.Sprintf("col-%v", i), typing.Invalid))
	}

	cm := &types.DestinationTableConfigMap{}
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("dusty_the_mini_aussie")

	tableCfg := types.NewDestinationTableConfig(cols, false)
	cm.AddTable(fakeTableID, tableCfg)

	actualTableCfg, err := GetTableCfgArgs{
		Destination: &mocks.FakeDestination{},
		TableID:     fakeTableID,
		ConfigMap:   cm,
	}.GetTableConfig(t.Context())

	assert.NoError(t, err)
	assert.Equal(t, tableCfg, actualTableCfg)
}
