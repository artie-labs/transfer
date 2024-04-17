package shared

import (
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/stretchr/testify/assert"
)

type MockDWH struct{}

func (MockDWH) Label() constants.DestinationKind                   { panic("not implemented") }
func (MockDWH) Merge(tableData *optimization.TableData) error      { panic("not implemented") }
func (MockDWH) Append(tableData *optimization.TableData) error     { panic("not implemented") }
func (MockDWH) Dedupe(tableID optimization.TableIdentifier) error  { panic("not implemented") }
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

func (m MockDWH) ToFullyQualifiedName(tableID optimization.TableIdentifier, escape bool) string {
	return tableID.FqName(constants.Redshift, false, true, optimization.FqNameOpts{})
}

func TestTempTableName(t *testing.T) {
	tempTableName := TempTableName(MockDWH{}, optimization.NewTableIdentifier("db", "schema", "table"), "sUfFiX")

	expectedPrefix := "schema.table___artie_sUfFiX_"
	assert.True(t, strings.HasPrefix(tempTableName, expectedPrefix))

	// Check the TTL:
	suffix := tempTableName[len(expectedPrefix):]
	assert.Len(t, suffix, 10)
	epoch, err := strconv.ParseInt(suffix, 10, 64)
	assert.NoError(t, err)
	assert.Greater(t, time.Unix(epoch, 0), time.Now())
}
