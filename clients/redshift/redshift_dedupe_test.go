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

func (r *RedshiftTestSuite) Test_BuildDedupeChunkInsertQuery() {
	tableID := dialect.NewTableIdentifier("public", "customers")
	newTableID := dialect.NewTableIdentifier("public", "customers__artie_dedupe")
	{
		// Single PK, exclusive upper.
		query := dialect.RedshiftDialect{}.BuildDedupeChunkInsertQuery(tableID, newTableID, []string{"id"}, false, "id", false)
		assert.Equal(
			r.T(),
			`INSERT INTO public."customers__artie_dedupe" SELECT * FROM public."customers" WHERE "id" >= $1 AND "id" < $2 QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" DESC) = 1`,
			query,
		)
	}
	{
		// Single PK with __artie_updated_at, inclusive upper (last chunk).
		query := dialect.RedshiftDialect{}.BuildDedupeChunkInsertQuery(tableID, newTableID, []string{"id"}, true, "id", true)
		assert.Equal(
			r.T(),
			`INSERT INTO public."customers__artie_dedupe" SELECT * FROM public."customers" WHERE "id" >= $1 AND "id" <= $2 QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" DESC, "__artie_updated_at" DESC) = 1`,
			query,
		)
	}
	{
		// Composite PK; boundary key is the first PK, PARTITION BY covers the full PK.
		settingsTableID := dialect.NewTableIdentifier("public", "user_settings")
		newSettingsTableID := dialect.NewTableIdentifier("public", "user_settings__artie_dedupe")
		query := dialect.RedshiftDialect{}.BuildDedupeChunkInsertQuery(settingsTableID, newSettingsTableID, []string{"user_id", "settings"}, false, "user_id", false)
		assert.Equal(
			r.T(),
			`INSERT INTO public."user_settings__artie_dedupe" SELECT * FROM public."user_settings" WHERE "user_id" >= $1 AND "user_id" < $2 QUALIFY ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "user_id" DESC, "settings" DESC) = 1`,
			query,
		)
	}
}

func (r *RedshiftTestSuite) Test_BuildDedupeNullChunkInsertQuery() {
	tableID := dialect.NewTableIdentifier("public", "customers")
	newTableID := dialect.NewTableIdentifier("public", "customers__artie_dedupe")
	{
		// Single PK, no __artie_updated_at.
		query := dialect.RedshiftDialect{}.BuildDedupeNullChunkInsertQuery(tableID, newTableID, []string{"id"}, false, "id")
		assert.Equal(
			r.T(),
			`INSERT INTO public."customers__artie_dedupe" SELECT * FROM public."customers" WHERE "id" IS NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" DESC) = 1`,
			query,
		)
	}
	{
		// Composite PK with __artie_updated_at.
		settingsTableID := dialect.NewTableIdentifier("public", "user_settings")
		newSettingsTableID := dialect.NewTableIdentifier("public", "user_settings__artie_dedupe")
		query := dialect.RedshiftDialect{}.BuildDedupeNullChunkInsertQuery(settingsTableID, newSettingsTableID, []string{"user_id", "settings"}, true, "user_id")
		assert.Equal(
			r.T(),
			`INSERT INTO public."user_settings__artie_dedupe" SELECT * FROM public."user_settings" WHERE "user_id" IS NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "user_id" DESC, "settings" DESC, "__artie_updated_at" DESC) = 1`,
			query,
		)
	}
}
