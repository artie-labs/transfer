package snowflake

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/stretchr/testify/assert"
)

func TestGenerateDedupeQueries(t *testing.T) {
	{
		// Dedupe with one primary key + no `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("db", "public", "customers")
		stagingTableID := shared.TempTableID(tableID)

		parts := dialect.SnowflakeDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"id"}, false)
		assert.Len(t, parts, 3)
		assert.Equal(
			t,
			fmt.Sprintf(`CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM "DB"."PUBLIC"."CUSTOMERS" QUALIFY ROW_NUMBER() OVER (PARTITION BY "ID" ORDER BY "ID" ASC) = 2)`, stagingTableID.FullyQualifiedName()),
			parts[0],
		)
		assert.Equal(t, fmt.Sprintf(`DELETE FROM "DB"."PUBLIC"."CUSTOMERS" t1 USING %s t2 WHERE t1."ID" = t2."ID"`, stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(t, fmt.Sprintf(`INSERT INTO "DB"."PUBLIC"."CUSTOMERS" SELECT * FROM %s`, stagingTableID.FullyQualifiedName()), parts[2])
	}
	{
		// Dedupe with one primary key + `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("db", "public", "customers")
		stagingTableID := shared.TempTableID(tableID)

		parts := dialect.SnowflakeDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"id"}, true)
		assert.Len(t, parts, 3)
		assert.Equal(
			t,
			fmt.Sprintf(`CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM "DB"."PUBLIC"."CUSTOMERS" QUALIFY ROW_NUMBER() OVER (PARTITION BY "ID" ORDER BY "ID" ASC, "__ARTIE_UPDATED_AT" ASC) = 2)`, stagingTableID.FullyQualifiedName()),
			parts[0],
		)
		assert.Equal(t, fmt.Sprintf(`DELETE FROM "DB"."PUBLIC"."CUSTOMERS" t1 USING %s t2 WHERE t1."ID" = t2."ID"`, stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(t, fmt.Sprintf(`INSERT INTO "DB"."PUBLIC"."CUSTOMERS" SELECT * FROM %s`, stagingTableID.FullyQualifiedName()), parts[2])
	}
	{
		// Dedupe with composite keys + no `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("db", "public", "user_settings")
		stagingTableID := shared.TempTableID(tableID)

		parts := dialect.SnowflakeDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, false)
		assert.Len(t, parts, 3)
		assert.Equal(
			t,
			fmt.Sprintf(`CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM "DB"."PUBLIC"."USER_SETTINGS" QUALIFY ROW_NUMBER() OVER (PARTITION BY "USER_ID", "SETTINGS" ORDER BY "USER_ID" ASC, "SETTINGS" ASC) = 2)`, stagingTableID.FullyQualifiedName()),
			parts[0],
		)
		assert.Equal(t, fmt.Sprintf(`DELETE FROM "DB"."PUBLIC"."USER_SETTINGS" t1 USING %s t2 WHERE t1."USER_ID" = t2."USER_ID" AND t1."SETTINGS" = t2."SETTINGS"`, stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(t, fmt.Sprintf(`INSERT INTO "DB"."PUBLIC"."USER_SETTINGS" SELECT * FROM %s`, stagingTableID.FullyQualifiedName()), parts[2])
	}
	{
		// Dedupe with composite keys + `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("db", "public", "user_settings")
		stagingTableID := shared.TempTableID(tableID)

		parts := dialect.SnowflakeDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, true)
		assert.Len(t, parts, 3)
		assert.Equal(
			t,
			fmt.Sprintf(`CREATE OR REPLACE TRANSIENT TABLE %s AS (SELECT * FROM "DB"."PUBLIC"."USER_SETTINGS" QUALIFY ROW_NUMBER() OVER (PARTITION BY "USER_ID", "SETTINGS" ORDER BY "USER_ID" ASC, "SETTINGS" ASC, "__ARTIE_UPDATED_AT" ASC) = 2)`, stagingTableID.FullyQualifiedName()),
			parts[0],
		)
		assert.Equal(t, fmt.Sprintf(`DELETE FROM "DB"."PUBLIC"."USER_SETTINGS" t1 USING %s t2 WHERE t1."USER_ID" = t2."USER_ID" AND t1."SETTINGS" = t2."SETTINGS"`, stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(t, fmt.Sprintf(`INSERT INTO "DB"."PUBLIC"."USER_SETTINGS" SELECT * FROM %s`, stagingTableID.FullyQualifiedName()), parts[2])
	}
}
