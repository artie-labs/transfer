package shared

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
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

type MockDWH struct{}

func (MockDWH) Label() constants.DestinationKind                   { panic("not implemented") }
func (MockDWH) Merge(tableData *optimization.TableData) error      { panic("not implemented") }
func (MockDWH) Append(tableData *optimization.TableData) error     { panic("not implemented") }
func (MockDWH) Dedupe(tableID types.TableIdentifier) error         { panic("not implemented") }
func (MockDWH) Exec(query string, args ...any) (sql.Result, error) { panic("not implemented") }
func (MockDWH) Query(query string, args ...any) (*sql.Rows, error) { panic("not implemented") }
func (MockDWH) Begin() (*sql.Tx, error)                            { panic("not implemented") }
func (MockDWH) IsRetryableError(err error) bool                    { panic("not implemented") }
func (MockDWH) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	panic("not implemented")
}
func (MockDWH) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	panic("not implemented")
}
func (MockDWH) IdentifierFor(topicConfig kafkalib.TopicConfig, name string) types.TableIdentifier {
	panic("not implemented")
}
func (MockDWH) ShouldUppercaseEscapedNames() bool { return true }

type MockTableIdentifier struct{ fqName string }

func (MockTableIdentifier) Table() string                                { panic("not implemented") }
func (MockTableIdentifier) WithTable(table string) types.TableIdentifier { panic("not implemented") }
func (m MockTableIdentifier) FullyQualifiedName(_ bool) string           { return m.fqName }

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

	actualTableCfg, err := GetTableCfgArgs{
		Dwh:       MockDWH{},
		TableID:   MockTableIdentifier{fqName},
		ConfigMap: cm,
	}.GetTableConfig()

	assert.NoError(t, err)
	assert.Equal(t, dwhTableCfg, actualTableCfg)
}
