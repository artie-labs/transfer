package bigquery

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/stretchr/testify/assert"
)

func (b *BigQueryTestSuite) TestTableRelName() {
	{
		relName, err := tableRelName("project.dataset.table")
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), "table", relName)
	}
	{
		relName, err := tableRelName("project.dataset.table.table")
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), "table.table", relName)
	}
	{
		// All the possible errors
		_, err := tableRelName("project.dataset")
		assert.ErrorContains(b.T(), err, "invalid fully qualified name: project.dataset")

		_, err = tableRelName("project")
		assert.ErrorContains(b.T(), err, "invalid fully qualified name: project")
	}
}

func TestFullyQualifiedName(t *testing.T) {
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "database", Schema: "schema"}, "table")

	{
		// With UppercaseEscapedNames: true
		store := Store{
			config: config.Config{
				BigQuery: &config.BigQuery{
					ProjectID: "project",
				},
				SharedDestinationConfig: config.SharedDestinationConfig{
					UppercaseEscapedNames: true,
				},
			},
		}
		assert.Equal(t, "`project`.`database`.`TABLE`", store.ToFullyQualifiedName(tableData, true), "escaped")
		assert.Equal(t, "`project`.`database`.table", store.ToFullyQualifiedName(tableData, false), "unescaped")
	}
	{
		// With UppercaseEscapedNames: false
		store := Store{
			config: config.Config{
				BigQuery: &config.BigQuery{
					ProjectID: "project",
				},
				SharedDestinationConfig: config.SharedDestinationConfig{
					UppercaseEscapedNames: false,
				},
			},
		}
		assert.Equal(t, "`project`.`database`.`table`", store.ToFullyQualifiedName(tableData, true), "escaped")
		assert.Equal(t, "`project`.`database`.table", store.ToFullyQualifiedName(tableData, false), "unescaped")
	}
	{
		store := Store{
			config: config.Config{
				BigQuery: &config.BigQuery{
					ProjectID: "artie",
				},
			},
		}
		td := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", Schema: "public"}, "food")
		assert.Equal(t, "food", td.Name())
		assert.Equal(t, "`artie`.`db`.food", store.ToFullyQualifiedName(td, true))
	}
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
	tempTableName := shared.TempTableName(&Store{config: config.Config{BigQuery: &config.BigQuery{ProjectID: "123454321"}}}, tableData, "sUfFiX")
	assert.Equal(t, "`123454321`.`db`.table___artie_sUfFiX", trimTTL(tempTableName))
}
