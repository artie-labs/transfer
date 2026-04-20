package redshift

import (
	"fmt"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/redshift/dialect"
	"github.com/artie-labs/transfer/clients/shared"
)

func (r *RedshiftTestSuite) Test_GenerateDedupeQueries() {
	{
		// Dedupe with one primary key + no `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "customers")
		stagingTableID := shared.TempTableID(r.store, tableID)

		parts := dialect.RedshiftDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"id"}, false)
		assert.Len(r.T(), parts, 3)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" AS (SELECT * FROM public."customers" WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" ASC) = 2)`, stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(r.T(), fmt.Sprintf(`DELETE FROM public."customers" USING "%s" t2 WHERE "customers"."id" = t2."id"`, stagingTableID.Table()), parts[1])
		assert.Equal(r.T(), fmt.Sprintf(`INSERT INTO public."customers" SELECT * FROM "%s"`, stagingTableID.Table()), parts[2])
	}
	{
		// Dedupe with one primary key + `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "customers")
		stagingTableID := shared.TempTableID(r.store, tableID)

		parts := dialect.RedshiftDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"id"}, true)
		assert.Len(r.T(), parts, 3)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" AS (SELECT * FROM public."customers" WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" ASC, "__artie_updated_at" ASC) = 2)`, stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(r.T(), fmt.Sprintf(`DELETE FROM public."customers" USING "%s" t2 WHERE "customers"."id" = t2."id"`, stagingTableID.Table()), parts[1])
		assert.Equal(r.T(), fmt.Sprintf(`INSERT INTO public."customers" SELECT * FROM "%s"`, stagingTableID.Table()), parts[2])
	}
	{
		// Dedupe with composite keys + no `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "user_settings")
		stagingTableID := shared.TempTableID(r.store, tableID)

		parts := dialect.RedshiftDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, false)
		assert.Len(r.T(), parts, 3)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" AS (SELECT * FROM public."user_settings" WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "user_id" ASC, "settings" ASC) = 2)`, stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(r.T(), fmt.Sprintf(`DELETE FROM public."user_settings" USING "%s" t2 WHERE "user_settings"."user_id" = t2."user_id" AND "user_settings"."settings" = t2."settings"`, stagingTableID.Table()), parts[1])
		assert.Equal(r.T(), fmt.Sprintf(`INSERT INTO public."user_settings" SELECT * FROM "%s"`, stagingTableID.Table()), parts[2])
	}
	{
		// Dedupe with composite keys + `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "user_settings")
		stagingTableID := shared.TempTableID(r.store, tableID)

		parts := dialect.RedshiftDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, true)
		assert.Len(r.T(), parts, 3)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" AS (SELECT * FROM public."user_settings" WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "user_id" ASC, "settings" ASC, "__artie_updated_at" ASC) = 2)`, stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(r.T(), fmt.Sprintf(`DELETE FROM public."user_settings" USING "%s" t2 WHERE "user_settings"."user_id" = t2."user_id" AND "user_settings"."settings" = t2."settings"`, stagingTableID.Table()), parts[1])
		assert.Equal(r.T(), fmt.Sprintf(`INSERT INTO public."user_settings" SELECT * FROM "%s"`, stagingTableID.Table()), parts[2])
	}
}

func (r *RedshiftTestSuite) Test_GenerateDedupeQueriesFixed() {
	{
		// Single PK, no `__artie_updated_at`.
		tableID := dialect.NewTableIdentifier("public", "customers")
		stagingTableID := shared.TempTableID(r.store, tableID)
		columns := []string{"id", "first_name", "last_name"}

		parts := dialect.RedshiftDialect{}.BuildDedupeQueriesFixed(tableID, stagingTableID, []string{"id"}, false, columns)
		assert.Len(r.T(), parts, 4)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" (LIKE public."customers", "_artie_dedupe_rn" BIGINT IDENTITY(1,1))`, stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`INSERT INTO "%s" ("id", "first_name", "last_name") SELECT "id", "first_name", "last_name" FROM public."customers" WHERE ("id") IN (SELECT "id" FROM public."customers" GROUP BY "id" HAVING COUNT(*) > 1)`, stagingTableID.Table()),
			parts[1],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`DELETE FROM public."customers" USING "%s" t2 WHERE "customers"."id" = t2."id"`, stagingTableID.Table()),
			parts[2],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`INSERT INTO public."customers" ("id", "first_name", "last_name") SELECT "id", "first_name", "last_name" FROM "%s" WHERE "_artie_dedupe_rn" IN (SELECT "_artie_dedupe_rn" FROM "%s" QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" ASC, "_artie_dedupe_rn" ASC) = 2)`, stagingTableID.Table(), stagingTableID.Table()),
			parts[3],
		)
	}
	{
		// Single PK + `__artie_updated_at`.
		tableID := dialect.NewTableIdentifier("public", "customers")
		stagingTableID := shared.TempTableID(r.store, tableID)
		columns := []string{"id", "first_name", "last_name", "__artie_updated_at"}

		parts := dialect.RedshiftDialect{}.BuildDedupeQueriesFixed(tableID, stagingTableID, []string{"id"}, true, columns)
		assert.Len(r.T(), parts, 4)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" (LIKE public."customers", "_artie_dedupe_rn" BIGINT IDENTITY(1,1))`, stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`INSERT INTO "%s" ("id", "first_name", "last_name", "__artie_updated_at") SELECT "id", "first_name", "last_name", "__artie_updated_at" FROM public."customers" WHERE ("id") IN (SELECT "id" FROM public."customers" GROUP BY "id" HAVING COUNT(*) > 1)`, stagingTableID.Table()),
			parts[1],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`DELETE FROM public."customers" USING "%s" t2 WHERE "customers"."id" = t2."id"`, stagingTableID.Table()),
			parts[2],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`INSERT INTO public."customers" ("id", "first_name", "last_name", "__artie_updated_at") SELECT "id", "first_name", "last_name", "__artie_updated_at" FROM "%s" WHERE "_artie_dedupe_rn" IN (SELECT "_artie_dedupe_rn" FROM "%s" QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" ASC, "__artie_updated_at" ASC, "_artie_dedupe_rn" ASC) = 2)`, stagingTableID.Table(), stagingTableID.Table()),
			parts[3],
		)
	}
	{
		// Composite PK, no `__artie_updated_at`.
		tableID := dialect.NewTableIdentifier("public", "user_settings")
		stagingTableID := shared.TempTableID(r.store, tableID)
		columns := []string{"user_id", "settings", "value"}

		parts := dialect.RedshiftDialect{}.BuildDedupeQueriesFixed(tableID, stagingTableID, []string{"user_id", "settings"}, false, columns)
		assert.Len(r.T(), parts, 4)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" (LIKE public."user_settings", "_artie_dedupe_rn" BIGINT IDENTITY(1,1))`, stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`INSERT INTO "%s" ("user_id", "settings", "value") SELECT "user_id", "settings", "value" FROM public."user_settings" WHERE ("user_id", "settings") IN (SELECT "user_id", "settings" FROM public."user_settings" GROUP BY "user_id", "settings" HAVING COUNT(*) > 1)`, stagingTableID.Table()),
			parts[1],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`DELETE FROM public."user_settings" USING "%s" t2 WHERE "user_settings"."user_id" = t2."user_id" AND "user_settings"."settings" = t2."settings"`, stagingTableID.Table()),
			parts[2],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`INSERT INTO public."user_settings" ("user_id", "settings", "value") SELECT "user_id", "settings", "value" FROM "%s" WHERE "_artie_dedupe_rn" IN (SELECT "_artie_dedupe_rn" FROM "%s" QUALIFY ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "user_id" ASC, "settings" ASC, "_artie_dedupe_rn" ASC) = 2)`, stagingTableID.Table(), stagingTableID.Table()),
			parts[3],
		)
	}
	{
		// Composite PK + `__artie_updated_at`.
		tableID := dialect.NewTableIdentifier("public", "user_settings")
		stagingTableID := shared.TempTableID(r.store, tableID)
		columns := []string{"user_id", "settings", "value", "__artie_updated_at"}

		parts := dialect.RedshiftDialect{}.BuildDedupeQueriesFixed(tableID, stagingTableID, []string{"user_id", "settings"}, true, columns)
		assert.Len(r.T(), parts, 4)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" (LIKE public."user_settings", "_artie_dedupe_rn" BIGINT IDENTITY(1,1))`, stagingTableID.Table()),
			parts[0],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`INSERT INTO "%s" ("user_id", "settings", "value", "__artie_updated_at") SELECT "user_id", "settings", "value", "__artie_updated_at" FROM public."user_settings" WHERE ("user_id", "settings") IN (SELECT "user_id", "settings" FROM public."user_settings" GROUP BY "user_id", "settings" HAVING COUNT(*) > 1)`, stagingTableID.Table()),
			parts[1],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`DELETE FROM public."user_settings" USING "%s" t2 WHERE "user_settings"."user_id" = t2."user_id" AND "user_settings"."settings" = t2."settings"`, stagingTableID.Table()),
			parts[2],
		)
		assert.Equal(
			r.T(),
			fmt.Sprintf(`INSERT INTO public."user_settings" ("user_id", "settings", "value", "__artie_updated_at") SELECT "user_id", "settings", "value", "__artie_updated_at" FROM "%s" WHERE "_artie_dedupe_rn" IN (SELECT "_artie_dedupe_rn" FROM "%s" QUALIFY ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "user_id" ASC, "settings" ASC, "__artie_updated_at" ASC, "_artie_dedupe_rn" ASC) = 2)`, stagingTableID.Table(), stagingTableID.Table()),
			parts[3],
		)
	}
}
