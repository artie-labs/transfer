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
		// Single PK.
		tableID := dialect.NewTableIdentifier("public", "customers")
		stagingTableID := shared.TempTableID(r.store, tableID)

		plan := dialect.RedshiftDialect{}.BuildDedupeQueriesFixed(tableID, stagingTableID, []string{"id"})
		// Prep: single CREATE statement. Intentionally no DROP IF EXISTS —
		// leftover _dedupe must surface rather than get silently clobbered.
		assert.Len(r.T(), plan.Prep, 1)
		assert.Equal(r.T(), `CREATE TABLE public."customers___artie_dedupe" (LIKE public."customers" INCLUDING DEFAULTS, "_artie_dedupe_rn" BIGINT IDENTITY(1,1))`, plan.Prep[0])
		// Append: single statement, auto-commit (ALTER TABLE APPEND cannot run in a txn).
		assert.Equal(r.T(), `ALTER TABLE public."customers___artie_dedupe" APPEND FROM public."customers" FILLTARGET`, plan.Append)
		// Swap: 5 statements (txn).
		assert.Len(r.T(), plan.Swap, 5)
		assert.Equal(r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" DISTSTYLE ALL AS SELECT "_artie_dedupe_rn" FROM public."customers___artie_dedupe" WHERE "_artie_dedupe_rn" NOT IN (SELECT MAX("_artie_dedupe_rn") FROM public."customers___artie_dedupe" GROUP BY "id")`, stagingTableID.Table()),
			plan.Swap[0])
		assert.Equal(r.T(),
			fmt.Sprintf(`DELETE FROM public."customers___artie_dedupe" USING "%s" l WHERE "customers___artie_dedupe"."_artie_dedupe_rn" = l."_artie_dedupe_rn"`, stagingTableID.Table()),
			plan.Swap[1])
		assert.Equal(r.T(), `ALTER TABLE public."customers___artie_dedupe" DROP COLUMN "_artie_dedupe_rn"`, plan.Swap[2])
		assert.Equal(r.T(), `DROP TABLE public."customers"`, plan.Swap[3])
		assert.Equal(r.T(), `ALTER TABLE public."customers___artie_dedupe" RENAME TO "customers"`, plan.Swap[4])
	}
	{
		// Composite PK.
		tableID := dialect.NewTableIdentifier("public", "user_settings")
		stagingTableID := shared.TempTableID(r.store, tableID)

		plan := dialect.RedshiftDialect{}.BuildDedupeQueriesFixed(tableID, stagingTableID, []string{"user_id", "settings"})
		assert.Len(r.T(), plan.Prep, 1)
		assert.Equal(r.T(), `CREATE TABLE public."user_settings___artie_dedupe" (LIKE public."user_settings" INCLUDING DEFAULTS, "_artie_dedupe_rn" BIGINT IDENTITY(1,1))`, plan.Prep[0])
		assert.Equal(r.T(), `ALTER TABLE public."user_settings___artie_dedupe" APPEND FROM public."user_settings" FILLTARGET`, plan.Append)
		assert.Len(r.T(), plan.Swap, 5)
		assert.Equal(r.T(),
			fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" DISTSTYLE ALL AS SELECT "_artie_dedupe_rn" FROM public."user_settings___artie_dedupe" WHERE "_artie_dedupe_rn" NOT IN (SELECT MAX("_artie_dedupe_rn") FROM public."user_settings___artie_dedupe" GROUP BY "user_id", "settings")`, stagingTableID.Table()),
			plan.Swap[0])
		assert.Equal(r.T(),
			fmt.Sprintf(`DELETE FROM public."user_settings___artie_dedupe" USING "%s" l WHERE "user_settings___artie_dedupe"."_artie_dedupe_rn" = l."_artie_dedupe_rn"`, stagingTableID.Table()),
			plan.Swap[1])
		assert.Equal(r.T(), `ALTER TABLE public."user_settings___artie_dedupe" DROP COLUMN "_artie_dedupe_rn"`, plan.Swap[2])
		assert.Equal(r.T(), `DROP TABLE public."user_settings"`, plan.Swap[3])
		assert.Equal(r.T(), `ALTER TABLE public."user_settings___artie_dedupe" RENAME TO "user_settings"`, plan.Swap[4])
	}
}
