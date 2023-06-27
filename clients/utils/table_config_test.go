package utils

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/dwh/types"
)

func TestGetTableConfig(t *testing.T) {
	// Return early because table is found in configMap.
	cols := &columns.Columns{}
	for i := 0; i < 100; i++ {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col-%v", i), typing.Invalid))
	}

	fqName := "dusty_the_mini_aussie"
	dwhTableCfg := types.NewDwhTableConfig(cols, nil, false, false)

	cm := &types.DwhToTablesConfigMap{}
	cm.AddTableToConfig(fqName, dwhTableCfg)

	actualTableCfg, err := GetTableConfig(context.Background(), GetTableCfgArgs{
		FqName:    fqName,
		ConfigMap: cm,
	})
	assert.NoError(t, err)
	assert.Equal(t, dwhTableCfg, actualTableCfg)
}
