package snowflake

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/kafkalib/partition"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
)

func retrieveTableNameFromCreateTable(t *testing.T, query string) string {
	t.Helper()
	parts := strings.Split(query, ".")
	assert.Len(t, parts, 3)

	tableNamePart := parts[2]
	tableNameParts := strings.Split(tableNamePart, " ")
	assert.True(t, len(tableNameParts) > 2, tableNamePart)
	return strings.ReplaceAll(tableNameParts[0], `"`, "")
}

func (s *SnowflakeTestSuite) identifierFor(tableData *optimization.TableData) sql.TableIdentifier {
	return s.stageStore.IdentifierFor(tableData.TopicConfig(), tableData.Name())
}

func (s *SnowflakeTestSuite) TestExecuteMergeExitEarly() {
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
}

func (s *SnowflakeTestSuite) TestStore_AdditionalEqualityStrings() {
	{
		// No additional equality strings:
		tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
		assert.Empty(s.T(), s.stageStore.additionalEqualityStrings(tableData))
	}
	{
		// Additional equality strings:
		topicConfig := kafkalib.TopicConfig{}
		topicConfig.AdditionalMergePredicates = []partition.MergePredicates{
			{PartitionField: "foo"},
			{PartitionField: "bar"},
		}
		tableData := optimization.NewTableData(nil, config.Replication, nil, topicConfig, "foo")
		actual := s.stageStore.additionalEqualityStrings(tableData)
		assert.Equal(s.T(), []string{`tgt."FOO" = stg."FOO"`, `tgt."BAR" = stg."BAR"`}, actual)
	}
}

func TestTempTableIDWithSuffix(t *testing.T) {
	trimTTL := func(tableName string) string {
		lastUnderscore := strings.LastIndex(tableName, "_")
		assert.GreaterOrEqual(t, lastUnderscore, 0)
		epoch, err := strconv.ParseInt(tableName[lastUnderscore+1:len(tableName)-1], 10, 64)
		assert.NoError(t, err)
		assert.Greater(t, time.Unix(epoch, 0), time.Now().Add(5*time.Hour)) // default TTL is 6 hours from now
		return tableName[:lastUnderscore] + string(tableName[len(tableName)-1])
	}

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", Schema: "schema"}, "table")
	tableID := (&Store{}).IdentifierFor(tableData.TopicConfig(), tableData.Name())
	tempTableName := shared.TempTableIDWithSuffix(tableID, "sUfFiX").FullyQualifiedName()
	assert.Equal(t, `"DB"."SCHEMA"."TABLE___ARTIE_SUFFIX"`, trimTTL(tempTableName))
}
