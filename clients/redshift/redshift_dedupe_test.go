package redshift

import (
	"fmt"

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
}

func (r *RedshiftTestSuite) Test_GenerateDedupeChunkedQueries() {
	{
		// Chunked dedupe with one primary key + no `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "customers")
		newTableID := dialect.NewTableIdentifier("public", "customers__artie_dedupe")

		parts := dialect.RedshiftDialect{}.BuildDedupeChunkedQueries(tableID, newTableID, []string{"id"}, false, 3)
		assert.Len(r.T(), parts, 7) // 1 drop + 1 create + 3 inserts + 1 drop + 1 rename
		assert.Equal(r.T(), `DROP TABLE IF EXISTS public."customers__artie_dedupe"`, parts[0])
		assert.Equal(r.T(), `CREATE TABLE public."customers__artie_dedupe" (LIKE public."customers")`, parts[1])
		for i := 1; i <= 3; i++ {
			assert.Equal(
				r.T(),
				fmt.Sprintf(
					`INSERT INTO public."customers__artie_dedupe" SELECT * FROM public."customers" WHERE true QUALIFY NTILE(3) OVER (ORDER BY "id") = %d AND ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" ASC) = 1`,
					i,
				),
				parts[i+1],
			)
		}
		assert.Equal(r.T(), `DROP TABLE IF EXISTS public."customers"`, parts[5])
		assert.Equal(r.T(), `ALTER TABLE public."customers__artie_dedupe" RENAME TO "customers"`, parts[6])
	}
	{
		// Chunked dedupe with one primary key + `__artie_updated_at` flag.
		tableID := dialect.NewTableIdentifier("public", "customers")
		newTableID := dialect.NewTableIdentifier("public", "customers__artie_dedupe")

		parts := dialect.RedshiftDialect{}.BuildDedupeChunkedQueries(tableID, newTableID, []string{"id"}, true, 2)
		assert.Len(r.T(), parts, 6) // 1 drop + 1 create + 2 inserts + 1 drop + 1 rename
		assert.Equal(r.T(), `DROP TABLE IF EXISTS public."customers__artie_dedupe"`, parts[0])
		assert.Equal(r.T(), `CREATE TABLE public."customers__artie_dedupe" (LIKE public."customers")`, parts[1])
		for i := 1; i <= 2; i++ {
			assert.Equal(
				r.T(),
				fmt.Sprintf(
					`INSERT INTO public."customers__artie_dedupe" SELECT * FROM public."customers" WHERE true QUALIFY NTILE(2) OVER (ORDER BY "id") = %d AND ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id" ASC, "__artie_updated_at" ASC) = 1`,
					i,
				),
				parts[i+1],
			)
		}
		assert.Equal(r.T(), `DROP TABLE IF EXISTS public."customers"`, parts[4])
		assert.Equal(r.T(), `ALTER TABLE public."customers__artie_dedupe" RENAME TO "customers"`, parts[5])
	}
	{
		// Chunked dedupe with composite keys.
		tableID := dialect.NewTableIdentifier("public", "user_settings")
		newTableID := dialect.NewTableIdentifier("public", "user_settings__artie_dedupe")

		parts := dialect.RedshiftDialect{}.BuildDedupeChunkedQueries(tableID, newTableID, []string{"user_id", "settings"}, false, 2)
		assert.Len(r.T(), parts, 6)
		assert.Equal(r.T(), `DROP TABLE IF EXISTS public."user_settings__artie_dedupe"`, parts[0])
		assert.Equal(r.T(), `CREATE TABLE public."user_settings__artie_dedupe" (LIKE public."user_settings")`, parts[1])
		for i := 1; i <= 2; i++ {
			assert.Equal(
				r.T(),
				fmt.Sprintf(
					`INSERT INTO public."user_settings__artie_dedupe" SELECT * FROM public."user_settings" WHERE true QUALIFY NTILE(2) OVER (ORDER BY "user_id", "settings") = %d AND ROW_NUMBER() OVER (PARTITION BY "user_id", "settings" ORDER BY "user_id" ASC, "settings" ASC) = 1`,
					i,
				),
				parts[i+1],
			)
		}
		assert.Equal(r.T(), `DROP TABLE IF EXISTS public."user_settings"`, parts[4])
		assert.Equal(r.T(), `ALTER TABLE public."user_settings__artie_dedupe" RENAME TO "user_settings"`, parts[5])
	}
}
