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

func TestTempTableName(t *testing.T) {
	trimTTL := func(tableName string) string {
		lastUnderscore := strings.LastIndex(tableName, "_")
		assert.GreaterOrEqual(t, lastUnderscore, 0)
		epoch, err := strconv.ParseInt(tableName[lastUnderscore+1:], 10, 64)
		assert.NoError(t, err)
		assert.Greater(t, time.Unix(epoch, 0), time.Now().Add(5*time.Hour)) // default TTL is 6 hours from now
		return tableName[:lastUnderscore]
	}

	store := Store{}
	{
		// Schema is "schema":
		tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", Schema: "schema"}, "table")
		tableID := store.IdentifierFor(tableData.TopicConfig(), tableData.Name())
		tempTableName := shared.TempTableID(tableID, "sUfFiX").FullyQualifiedName()
		assert.Equal(t, "schema.table___artie_sUfFiX", trimTTL(tempTableName))
	}
	{
		// Schema is "public" -> "dbo":
		tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", Schema: "public"}, "table")
		tableID := store.IdentifierFor(tableData.TopicConfig(), tableData.Name())
		tempTableName := shared.TempTableID(tableID, "sUfFiX").FullyQualifiedName()
		assert.Equal(t, "dbo.table___artie_sUfFiX", trimTTL(tempTableName))
	}
}
