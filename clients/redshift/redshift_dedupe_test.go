package redshift

import (
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/redshift/dialect"
)

func (r *RedshiftTestSuite) Test_GenerateDedupeQueries() {
	{
		// Dedupe with one primary key + no `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "customers")
		stagingTableID := dialect.NewTableIdentifier("public", "customers__artie_stg")

		parts := dialect.RedshiftDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"id"}, false)
		assert.Len(r.T(), parts, 3)
		assert.Equal(
			r.T(),
			`CREATE TEMPORARY TABLE "customers__artie_stg" AS (SELECT * FROM public."customers" WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" ASC) = 2)`,
			parts[0],
		)
		assert.Equal(r.T(), `DELETE FROM public."customers" USING "customers__artie_stg" t2 WHERE "customers"."id" = t2."id"`, parts[1])
		assert.Equal(r.T(), `INSERT INTO public."customers" SELECT * FROM "customers__artie_stg"`, parts[2])
	}
	{
		// Dedupe with one primary key + `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "customers")
		stagingTableID := dialect.NewTableIdentifier("public", "customers__artie_stg")

		parts := dialect.RedshiftDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"id"}, true)
		assert.Len(r.T(), parts, 3)
		assert.Equal(
			r.T(),
			`CREATE TEMPORARY TABLE "customers__artie_stg" AS (SELECT * FROM public."customers" WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" ASC, "__artie_updated_at" ASC) = 2)`,
			parts[0],
		)
		assert.Equal(r.T(), `DELETE FROM public."customers" USING "customers__artie_stg" t2 WHERE "customers"."id" = t2."id"`, parts[1])
		assert.Equal(r.T(), `INSERT INTO public."customers" SELECT * FROM "customers__artie_stg"`, parts[2])
	}
	{
		// Dedupe with composite keys + no `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "user_settings")
		stagingTableID := dialect.NewTableIdentifier("public", "user_settings__artie_stg")

		parts := dialect.RedshiftDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, false)
		assert.Len(r.T(), parts, 3)
		assert.Equal(
			r.T(),
			`CREATE TEMPORARY TABLE "user_settings__artie_stg" AS (SELECT * FROM public."user_settings" WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "user_id" ASC, "settings" ASC) = 2)`,
			parts[0],
		)
		assert.Equal(r.T(), `DELETE FROM public."user_settings" USING "user_settings__artie_stg" t2 WHERE "user_settings"."user_id" = t2."user_id" AND "user_settings"."settings" = t2."settings"`, parts[1])
		assert.Equal(r.T(), `INSERT INTO public."user_settings" SELECT * FROM "user_settings__artie_stg"`, parts[2])
	}
	{
		// Dedupe with composite keys + `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "user_settings")
		stagingTableID := dialect.NewTableIdentifier("public", "user_settings__artie_stg")

		parts := dialect.RedshiftDialect{}.BuildDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, true)
		assert.Len(r.T(), parts, 3)
		assert.Equal(
			r.T(),
			`CREATE TEMPORARY TABLE "user_settings__artie_stg" AS (SELECT * FROM public."user_settings" WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "user_id" ASC, "settings" ASC, "__artie_updated_at" ASC) = 2)`,
			parts[0],
		)
		assert.Equal(r.T(), `DELETE FROM public."user_settings" USING "user_settings__artie_stg" t2 WHERE "user_settings"."user_id" = t2."user_id" AND "user_settings"."settings" = t2."settings"`, parts[1])
		assert.Equal(r.T(), `INSERT INTO public."user_settings" SELECT * FROM "user_settings__artie_stg"`, parts[2])
	}
}

func (r *RedshiftTestSuite) Test_BuildDedupeBoundaryQuery() {
	tableID := dialect.NewTableIdentifier("public", "customers")
	{
		// 2 chunks -> MIN + 1 percentile (0.5) + MAX.
		query := dialect.RedshiftDialect{}.BuildDedupeBoundaryQuery(tableID, "id", 2)
		assert.Equal(
			r.T(),
			`SELECT MIN("id"), APPROXIMATE PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY "id"), MAX("id") FROM public."customers"`,
			query,
		)
	}
	{
		// 4 chunks -> MIN + 3 percentiles (0.25, 0.5, 0.75) + MAX.
		query := dialect.RedshiftDialect{}.BuildDedupeBoundaryQuery(tableID, "id", 4)
		assert.Equal(
			r.T(),
			`SELECT MIN("id"), APPROXIMATE PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY "id"), APPROXIMATE PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY "id"), APPROXIMATE PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY "id"), MAX("id") FROM public."customers"`,
			query,
		)
	}
}

func (r *RedshiftTestSuite) Test_BuildDedupeStageCreateQuery() {
	tableID := dialect.NewTableIdentifier("public", "customers")
	stageID := dialect.NewTableIdentifier("public", "customers__artie_dedupe_stg")
	query := dialect.RedshiftDialect{}.BuildDedupeStageCreateQuery(stageID, tableID)
	assert.Equal(
		r.T(),
		`CREATE TEMPORARY TABLE "customers__artie_dedupe_stg" (LIKE public."customers", "_artie_dedupe_rn" BIGINT IDENTITY(1,1))`,
		query,
	)
}

func (r *RedshiftTestSuite) Test_BuildDedupeStageDropAndTruncateQueries() {
	stageID := dialect.NewTableIdentifier("public", "customers__artie_dedupe_stg")
	assert.Equal(
		r.T(),
		`DROP TABLE IF EXISTS "customers__artie_dedupe_stg"`,
		dialect.RedshiftDialect{}.BuildDedupeStageDropQuery(stageID),
	)
	assert.Equal(
		r.T(),
		`TRUNCATE TABLE "customers__artie_dedupe_stg"`,
		dialect.RedshiftDialect{}.BuildDedupeStageTruncateQuery(stageID),
	)
}

func (r *RedshiftTestSuite) Test_BuildDedupeStagePopulateRangeQuery() {
	tableID := dialect.NewTableIdentifier("public", "customers")
	stageID := dialect.NewTableIdentifier("public", "customers__artie_dedupe_stg")
	columns := []string{"id", "name", "meta"}
	{
		// Exclusive upper bound.
		query := dialect.RedshiftDialect{}.BuildDedupeStagePopulateRangeQuery(stageID, tableID, columns, "id", false)
		assert.Equal(
			r.T(),
			`INSERT INTO "customers__artie_dedupe_stg" ("id", "name", "meta") SELECT "id", "name", "meta" FROM public."customers" WHERE "id" >= $1 AND "id" < $2`,
			query,
		)
	}
	{
		// Inclusive upper bound (last chunk).
		query := dialect.RedshiftDialect{}.BuildDedupeStagePopulateRangeQuery(stageID, tableID, columns, "id", true)
		assert.Equal(
			r.T(),
			`INSERT INTO "customers__artie_dedupe_stg" ("id", "name", "meta") SELECT "id", "name", "meta" FROM public."customers" WHERE "id" >= $1 AND "id" <= $2`,
			query,
		)
	}
}

func (r *RedshiftTestSuite) Test_BuildDedupeStagePopulateNullQuery() {
	tableID := dialect.NewTableIdentifier("public", "customers")
	stageID := dialect.NewTableIdentifier("public", "customers__artie_dedupe_stg")
	columns := []string{"id", "name", "meta"}
	query := dialect.RedshiftDialect{}.BuildDedupeStagePopulateNullQuery(stageID, tableID, columns, "id")
	assert.Equal(
		r.T(),
		`INSERT INTO "customers__artie_dedupe_stg" ("id", "name", "meta") SELECT "id", "name", "meta" FROM public."customers" WHERE "id" IS NULL`,
		query,
	)
}

func (r *RedshiftTestSuite) Test_BuildDedupeStageWinnersInsertQuery() {
	newTableID := dialect.NewTableIdentifier("public", "customers__artie_dedupe")
	stageID := dialect.NewTableIdentifier("public", "customers__artie_dedupe_stg")
	columns := []string{"id", "name", "meta"}
	{
		// Single PK, no __artie_updated_at.
		query := dialect.RedshiftDialect{}.BuildDedupeStageWinnersInsertQuery(newTableID, stageID, columns, []string{"id"}, false)
		assert.Equal(
			r.T(),
			`INSERT INTO public."customers__artie_dedupe" ("id", "name", "meta") SELECT "id", "name", "meta" FROM "customers__artie_dedupe_stg" WHERE "_artie_dedupe_rn" IN (SELECT "_artie_dedupe_rn" FROM "customers__artie_dedupe_stg" QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "_artie_dedupe_rn" ASC) = 1)`,
			query,
		)
	}
	{
		// Single PK with __artie_updated_at.
		query := dialect.RedshiftDialect{}.BuildDedupeStageWinnersInsertQuery(newTableID, stageID, columns, []string{"id"}, true)
		assert.Equal(
			r.T(),
			`INSERT INTO public."customers__artie_dedupe" ("id", "name", "meta") SELECT "id", "name", "meta" FROM "customers__artie_dedupe_stg" WHERE "_artie_dedupe_rn" IN (SELECT "_artie_dedupe_rn" FROM "customers__artie_dedupe_stg" QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "__artie_updated_at" DESC, "_artie_dedupe_rn" ASC) = 1)`,
			query,
		)
	}
	{
		// Composite PK with __artie_updated_at.
		settingsTableID := dialect.NewTableIdentifier("public", "user_settings__artie_dedupe")
		settingsStageID := dialect.NewTableIdentifier("public", "user_settings__artie_dedupe_stg")
		cols := []string{"user_id", "settings", "value"}
		query := dialect.RedshiftDialect{}.BuildDedupeStageWinnersInsertQuery(settingsTableID, settingsStageID, cols, []string{"user_id", "settings"}, true)
		assert.Equal(
			r.T(),
			`INSERT INTO public."user_settings__artie_dedupe" ("user_id", "settings", "value") SELECT "user_id", "settings", "value" FROM "user_settings__artie_dedupe_stg" WHERE "_artie_dedupe_rn" IN (SELECT "_artie_dedupe_rn" FROM "user_settings__artie_dedupe_stg" QUALIFY ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "__artie_updated_at" DESC, "_artie_dedupe_rn" ASC) = 1)`,
			query,
		)
	}
}
