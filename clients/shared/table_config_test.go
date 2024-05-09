package shared

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	sqllib "github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
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

func (MockDWH) Label() constants.DestinationKind               { panic("not implemented") }
func (MockDWH) Dialect() sqllib.Dialect                        { panic("not implemented") }
func (MockDWH) AdditionalDateFormats() []string                { panic("not implemented") }
func (MockDWH) Merge(tableData *optimization.TableData) error  { panic("not implemented") }
func (MockDWH) Append(tableData *optimization.TableData) error { panic("not implemented") }
func (MockDWH) Dedupe(tableID types.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) error {
	panic("not implemented")
}
func (MockDWH) Exec(query string, args ...any) (sql.Result, error) { panic("not implemented") }
func (MockDWH) Query(query string, args ...any) (*sql.Rows, error) { panic("not implemented") }
func (MockDWH) Begin() (*sql.Tx, error)                            { panic("not implemented") }
func (MockDWH) IsRetryableError(err error) bool                    { panic("not implemented") }
func (MockDWH) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	panic("not implemented")
}
func (MockDWH) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID types.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	panic("not implemented")
}
func (MockDWH) IdentifierFor(topicConfig kafkalib.TopicConfig, name string) types.TableIdentifier {
	panic("not implemented")
}

// TODO: Move this to mocks.
type MockTableIdentifier struct{ fqName string }

func (MockTableIdentifier) EscapedTable() string                         { panic("not implemented") }
func (MockTableIdentifier) Table() string                                { panic("not implemented") }
func (MockTableIdentifier) WithTable(table string) types.TableIdentifier { panic("not implemented") }
func (m MockTableIdentifier) FullyQualifiedName() string                 { return m.fqName }

func TestGetTableConfig(t *testing.T) {
	// Return early because table is found in configMap.
	cols := &columns.Columns{}
	for i := range 100 {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col-%v", i), typing.Invalid))
	}

	tableID := MockTableIdentifier{"dusty_the_mini_aussie"}
	dwhTableCfg := types.NewDwhTableConfig(cols, nil, false, false)

	cm := &types.DwhToTablesConfigMap{}
	cm.AddTableToConfig(tableID, dwhTableCfg)

	actualTableCfg, err := GetTableCfgArgs{
		Dwh:       MockDWH{},
		TableID:   tableID,
		ConfigMap: cm,
	}.GetTableConfig()

	assert.NoError(t, err)
	assert.Equal(t, dwhTableCfg, actualTableCfg)
}
