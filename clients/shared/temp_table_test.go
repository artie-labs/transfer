package shared

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/stretchr/testify/assert"
)

type MockDWH struct{}

func (MockDWH) Label() constants.DestinationKind                   { panic("not implemented") }
func (MockDWH) Merge(tableData *optimization.TableData) error      { panic("not implemented") }
func (MockDWH) Append(tableData *optimization.TableData) error     { panic("not implemented") }
func (MockDWH) Dedupe(tableData *optimization.TableData) error     { panic("not implemented") }
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
func (m MockDWH) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) types.TableIdentifier {
	panic("not implemented")
}

func (m MockDWH) ToFullyQualifiedName(tableData *optimization.TableData, escape bool) string {
	if escape {
		panic("escape should not be used")
	}
	return fmt.Sprintf("%s.%s.%s", tableData.TopicConfig.Database, tableData.TopicConfig.Schema, tableData.RawName())
}

func TestTempTableName(t *testing.T) {
	trimTTL := func(tableName string) string {
		lastUnderscore := strings.LastIndex(tableName, "_")
		assert.GreaterOrEqual(t, lastUnderscore, 0)
		epoch, err := strconv.ParseInt(tableName[lastUnderscore+1:], 10, 64)
		assert.NoError(t, err)
		assert.Greater(t, time.Unix(epoch, 0), time.Now().Add(5*time.Hour)) // default TTL is 6 hours from now
		return tableName[:lastUnderscore]
	}

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", Schema: "schema"}, "table")
	tempTableName := TempTableName(MockDWH{}, tableData, "sUfFiX")
	assert.Equal(t, "db.schema.table___artie_sUfFiX", trimTTL(tempTableName))
}
