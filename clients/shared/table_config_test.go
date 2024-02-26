package shared

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/destination/types"
)

func TestGetTableCfgArgs_ShouldParseComment(t *testing.T) {
	type _testCase struct {
		Name            string
		EmptyCommentVal *string
		Comment         string
		ExpectedResult  bool
	}

	testCases := []_testCase{
		{
			Name:           "empty comment val = nil",
			Comment:        "blah blah blah",
			ExpectedResult: true,
		},
		{
			Name:            "empty comment val = blah",
			EmptyCommentVal: ptr.ToString("blah"),
			Comment:         "blah",
		},
		{
			Name:            "empty comment val = blah2",
			EmptyCommentVal: ptr.ToString("blah2"),
			Comment:         "blah",
			ExpectedResult:  true,
		},
	}

	for _, testCase := range testCases {
		args := GetTableCfgArgs{
			EmptyCommentValue: testCase.EmptyCommentVal,
		}

		assert.Equal(t, testCase.ExpectedResult, args.ShouldParseComment(testCase.Comment), testCase.Name)
	}
}

func TestGetTableConfig(t *testing.T) {
	// Return early because table is found in configMap.
	cols := &columns.Columns{}
	for i := range 100 {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col-%v", i), typing.Invalid))
	}

	fqName := "dusty_the_mini_aussie"
	dwhTableCfg := types.NewDwhTableConfig(cols, nil, false, false)

	cm := &types.DwhToTablesConfigMap{}
	cm.AddTableToConfig(fqName, dwhTableCfg)

	actualTableCfg, err := GetTableConfig(GetTableCfgArgs{
		FqName:    fqName,
		ConfigMap: cm,
	})
	assert.NoError(t, err)
	assert.Equal(t, dwhTableCfg, actualTableCfg)
}
