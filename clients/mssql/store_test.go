package mssql

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

func TestFullyQualifiedName(t *testing.T) {
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "database", Schema: "schema"}, "table")

	{
		// With UppercaseEscapedNames: true
		store := Store{
			config: config.Config{
				SharedDestinationConfig: config.SharedDestinationConfig{
					UppercaseEscapedNames: true,
				},
			},
		}
		assert.Equal(t, `schema."TABLE"`, store.ToFullyQualifiedName(tableData, true), "escaped")
		assert.Equal(t, "schema.table", store.ToFullyQualifiedName(tableData, false), "unescaped")
	}
	{
		// With UppercaseEscapedNames: false
		store := Store{
			config: config.Config{
				SharedDestinationConfig: config.SharedDestinationConfig{
					UppercaseEscapedNames: false,
				},
			},
		}
		assert.Equal(t, `schema."table"`, store.ToFullyQualifiedName(tableData, true), "escaped")
		assert.Equal(t, "schema.table", store.ToFullyQualifiedName(tableData, false), "unescaped")
	}
	{
		td := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", Schema: "public"}, "food")
		assert.Equal(t, "food", td.Name())
		assert.Equal(t, "dbo.food", (&Store{}).ToFullyQualifiedName(td, true))
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

	{
		// Schema is "schema":
		tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", Schema: "schema"}, "table")
		tableID := (&Store{}).IdentifierFor(tableData.TopicConfig, tableData.Name())
		tempTableName := shared.TempTableName(&Store{}, tableID, "sUfFiX")
		assert.Equal(t, "schema.table___artie_sUfFiX", trimTTL(tempTableName))
	}
	{
		// Schema is "public" -> "dbo":
		tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", Schema: "public"}, "table")
		tableID := (&Store{}).IdentifierFor(tableData.TopicConfig, tableData.Name())
		tempTableName := shared.TempTableName(&Store{}, tableID, "sUfFiX")
		assert.Equal(t, "dbo.table___artie_sUfFiX", trimTTL(tempTableName))
	}
}
