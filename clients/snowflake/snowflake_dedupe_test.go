package snowflake

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/stretchr/testify/assert"
)

func (s *SnowflakeTestSuite) TestGenerateDedupeQueries() {
	{
		// Dedupe with one primary key + no `__artie_updated_at` flag.
		tableID := NewTableIdentifier("db", "public", "customers")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := s.stageStore.generateDedupeQueries(tableID, stagingTableID, []string{"id"}, kafkalib.TopicConfig{})
		assert.Len(s.T(), parts, 3)
		assert.Equal(
			s.T(),
			fmt.Sprintf(`CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM db.public."CUSTOMERS" QUALIFY ROW_NUMBER() OVER (PARTITION BY "ID" ORDER BY "ID" ASC) = 2)`, stagingTableID.FullyQualifiedName()),
			parts[0],
		)
		assert.Equal(s.T(), fmt.Sprintf(`DELETE FROM db.public."CUSTOMERS" t1 USING %s t2 WHERE t1."ID" = t2."ID"`, stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(s.T(), fmt.Sprintf(`INSERT INTO db.public."CUSTOMERS" SELECT * FROM %s`, stagingTableID.FullyQualifiedName()), parts[2])
	}
	{
		// Dedupe with one primary key + `__artie_updated_at` flag.
		tableID := NewTableIdentifier("db", "public", "customers")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := s.stageStore.generateDedupeQueries(tableID, stagingTableID, []string{"id"}, kafkalib.TopicConfig{IncludeArtieUpdatedAt: true})
		assert.Len(s.T(), parts, 3)
		assert.Equal(
			s.T(),
			fmt.Sprintf(`CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM db.public."CUSTOMERS" QUALIFY ROW_NUMBER() OVER (PARTITION BY "ID" ORDER BY "ID" ASC, "__ARTIE_UPDATED_AT" ASC) = 2)`, stagingTableID.FullyQualifiedName()),
			parts[0],
		)
		assert.Equal(s.T(), fmt.Sprintf(`DELETE FROM db.public."CUSTOMERS" t1 USING %s t2 WHERE t1."ID" = t2."ID"`, stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(s.T(), fmt.Sprintf(`INSERT INTO db.public."CUSTOMERS" SELECT * FROM %s`, stagingTableID.FullyQualifiedName()), parts[2])
	}
	{
		// Dedupe with composite keys + no `__artie_updated_at` flag.
		tableID := NewTableIdentifier("db", "public", "user_settings")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := s.stageStore.generateDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, kafkalib.TopicConfig{})
		assert.Len(s.T(), parts, 3)
		assert.Equal(
			s.T(),
			fmt.Sprintf(`CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM db.public."USER_SETTINGS" QUALIFY ROW_NUMBER() OVER (PARTITION BY "USER_ID", "SETTINGS" ORDER BY "USER_ID" ASC, "SETTINGS" ASC) = 2)`, stagingTableID.FullyQualifiedName()),
			parts[0],
		)
		assert.Equal(s.T(), fmt.Sprintf(`DELETE FROM db.public."USER_SETTINGS" t1 USING %s t2 WHERE t1."USER_ID" = t2."USER_ID" AND t1."SETTINGS" = t2."SETTINGS"`, stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(s.T(), fmt.Sprintf(`INSERT INTO db.public."USER_SETTINGS" SELECT * FROM %s`, stagingTableID.FullyQualifiedName()), parts[2])
	}
	{
		// Dedupe with composite keys + `__artie_updated_at` flag.
		tableID := NewTableIdentifier("db", "public", "user_settings")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := s.stageStore.generateDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, kafkalib.TopicConfig{IncludeArtieUpdatedAt: true})
		assert.Len(s.T(), parts, 3)
		assert.Equal(
			s.T(),
			fmt.Sprintf(`CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM db.public."USER_SETTINGS" QUALIFY ROW_NUMBER() OVER (PARTITION BY "USER_ID", "SETTINGS" ORDER BY "USER_ID" ASC, "SETTINGS" ASC, "__ARTIE_UPDATED_AT" ASC) = 2)`, stagingTableID.FullyQualifiedName()),
			parts[0],
		)
		assert.Equal(s.T(), fmt.Sprintf(`DELETE FROM db.public."USER_SETTINGS" t1 USING %s t2 WHERE t1."USER_ID" = t2."USER_ID" AND t1."SETTINGS" = t2."SETTINGS"`, stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(s.T(), fmt.Sprintf(`INSERT INTO db.public."USER_SETTINGS" SELECT * FROM %s`, stagingTableID.FullyQualifiedName()), parts[2])
	}
}
