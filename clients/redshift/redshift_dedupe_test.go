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
		stagingTableID := shared.TempTableID(tableID)

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
		stagingTableID := shared.TempTableID(tableID)

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
		stagingTableID := shared.TempTableID(tableID)

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
		stagingTableID := shared.TempTableID(tableID)

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
