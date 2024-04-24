package snowflake

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/stretchr/testify/assert"
)

func (s *SnowflakeTestSuite) TestDedupe() {
	{
		// Dedupe with one primary key + no `__artie_updated_at` flag.
		tableID := NewTableIdentifier("db", "public", "customers")
		tableData := optimization.NewTableData(nil, config.Replication, []string{"id"}, kafkalib.TopicConfig{}, "customers")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := s.stageStore.generateDedupeQueries(tableID, stagingTableID, tableData)
		assert.Len(s.T(), parts, 3)
		assert.Equal(
			s.T(),
			fmt.Sprintf("CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM db.public.customers QUALIFY ROW_NUMBER() OVER (PARTITION BY by id ORDER BY id ASC) = 2)", stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(s.T(), fmt.Sprintf("DELETE FROM db.public.customers t1 USING %s t2 WHERE t1.id = t2.id", stagingTableID.Table()), parts[1])
		assert.Equal(s.T(), fmt.Sprintf("INSERT INTO db.public.customers SELECT * FROM %s", stagingTableID.Table()), parts[2])
	}
	{
		// Dedupe with one primary key + `__artie_updated_at` flag.
		tableID := NewTableIdentifier("db", "public", "customers")
		tableData := optimization.NewTableData(nil, config.Replication, []string{"id"}, kafkalib.TopicConfig{
			IncludeArtieUpdatedAt: true,
		}, "customers")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := s.stageStore.generateDedupeQueries(tableID, stagingTableID, tableData)
		assert.Len(s.T(), parts, 3)
		assert.Equal(
			s.T(),
			fmt.Sprintf("CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM db.public.customers QUALIFY ROW_NUMBER() OVER (PARTITION BY by id ORDER BY id ASC, __artie_updated_at ASC) = 2)", stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(s.T(), fmt.Sprintf("DELETE FROM db.public.customers t1 USING %s t2 WHERE t1.id = t2.id", stagingTableID.Table()), parts[1])
		assert.Equal(s.T(), fmt.Sprintf("INSERT INTO db.public.customers SELECT * FROM %s", stagingTableID.Table()), parts[2])
	}
	{
		// Dedupe with composite keys + no `__artie_updated_at` flag.
		tableID := NewTableIdentifier("db", "public", "user_settings")
		tableData := optimization.NewTableData(nil, config.Replication, []string{"user_id", "settings"}, kafkalib.TopicConfig{}, "user_settings")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := s.stageStore.generateDedupeQueries(tableID, stagingTableID, tableData)
		assert.Len(s.T(), parts, 3)
		assert.Equal(
			s.T(),
			fmt.Sprintf("CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM db.public.user_settings QUALIFY ROW_NUMBER() OVER (PARTITION BY by user_id, settings ORDER BY user_id ASC, settings ASC) = 2)", stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(s.T(), fmt.Sprintf("DELETE FROM db.public.user_settings t1 USING %s t2 WHERE t1.user_id = t2.user_id, t1.settings = t2.settings", stagingTableID.Table()), parts[1])
		assert.Equal(s.T(), fmt.Sprintf("INSERT INTO db.public.user_settings SELECT * FROM %s", stagingTableID.Table()), parts[2])
	}
	{
		// Dedupe with composite keys + `__artie_updated_at` flag.
		tableID := NewTableIdentifier("db", "public", "user_settings")
		tableData := optimization.NewTableData(nil, config.Replication, []string{"user_id", "settings"}, kafkalib.TopicConfig{
			IncludeArtieUpdatedAt: true,
		}, "user_settings")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := s.stageStore.generateDedupeQueries(tableID, stagingTableID, tableData)
		assert.Len(s.T(), parts, 3)
		assert.Equal(
			s.T(),
			fmt.Sprintf("CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM db.public.user_settings QUALIFY ROW_NUMBER() OVER (PARTITION BY by user_id, settings ORDER BY user_id ASC, settings ASC, __artie_updated_at ASC) = 2)", stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(s.T(), fmt.Sprintf("DELETE FROM db.public.user_settings t1 USING %s t2 WHERE t1.user_id = t2.user_id, t1.settings = t2.settings", stagingTableID.Table()), parts[1])
		assert.Equal(s.T(), fmt.Sprintf("INSERT INTO db.public.user_settings SELECT * FROM %s", stagingTableID.Table()), parts[2])
	}
}
