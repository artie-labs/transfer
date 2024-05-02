package bigquery

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
)

func (b *BigQueryTestSuite) TestGenerateDedupeQueries() {
	{
		// Dedupe with one primary key + no `__artie_updated_at` flag.
		tableID := NewTableIdentifier("project12", "public", "customers")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := b.store.generateDedupeQueries(tableID, stagingTableID, []string{"id"}, kafkalib.TopicConfig{})
		assert.Len(b.T(), parts, 3)
		assert.Equal(
			b.T(),
			fmt.Sprintf("CREATE OR REPLACE TABLE %s OPTIONS (expiration_timestamp = TIMESTAMP(%s)) AS (SELECT * FROM `project12`.`public`.`customers` QUALIFY ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `id` ASC) = 2)",
				stagingTableID.FullyQualifiedName(),
				fmt.Sprintf(`"%s"`, typing.ExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL))),
			),
			parts[0],
		)
		assert.Equal(b.T(), fmt.Sprintf("DELETE FROM `project12`.`public`.`customers` t1 WHERE EXISTS (SELECT * FROM %s t2 WHERE t1.`id` = t2.`id`)", stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(b.T(), fmt.Sprintf("INSERT INTO `project12`.`public`.`customers` SELECT * FROM %s", stagingTableID.FullyQualifiedName()), parts[2])
	}
	{
		// Dedupe with one primary key + `__artie_updated_at` flag.
		tableID := NewTableIdentifier("project12", "public", "customers")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := b.store.generateDedupeQueries(tableID, stagingTableID, []string{"id"}, kafkalib.TopicConfig{IncludeArtieUpdatedAt: true})
		assert.Len(b.T(), parts, 3)
		assert.Equal(
			b.T(),
			fmt.Sprintf("CREATE OR REPLACE TABLE %s OPTIONS (expiration_timestamp = TIMESTAMP(%s)) AS (SELECT * FROM `project12`.`public`.`customers` QUALIFY ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `id` ASC, `__artie_updated_at` ASC) = 2)",
				stagingTableID.FullyQualifiedName(),
				fmt.Sprintf(`"%s"`, typing.ExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL))),
			),
			parts[0],
		)
		assert.Equal(b.T(), fmt.Sprintf("DELETE FROM `project12`.`public`.`customers` t1 WHERE EXISTS (SELECT * FROM %s t2 WHERE t1.`id` = t2.`id`)", stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(b.T(), fmt.Sprintf("INSERT INTO `project12`.`public`.`customers` SELECT * FROM %s", stagingTableID.FullyQualifiedName()), parts[2])
	}
	{
		// Dedupe with composite keys + no `__artie_updated_at` flag.
		tableID := NewTableIdentifier("project123", "public", "user_settings")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := b.store.generateDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, kafkalib.TopicConfig{})
		assert.Len(b.T(), parts, 3)
		assert.Equal(
			b.T(),
			fmt.Sprintf("CREATE OR REPLACE TABLE %s OPTIONS (expiration_timestamp = TIMESTAMP(%s)) AS (SELECT * FROM `project123`.`public`.`user_settings` QUALIFY ROW_NUMBER() OVER (PARTITION BY `user_id`, `settings` ORDER BY `user_id` ASC, `settings` ASC) = 2)",
				stagingTableID.FullyQualifiedName(),
				fmt.Sprintf(`"%s"`, typing.ExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL))),
			),
			parts[0],
		)
		assert.Equal(b.T(), fmt.Sprintf("DELETE FROM `project123`.`public`.`user_settings` t1 WHERE EXISTS (SELECT * FROM %s t2 WHERE t1.`user_id` = t2.`user_id` AND t1.`settings` = t2.`settings`)", stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(b.T(), fmt.Sprintf("INSERT INTO `project123`.`public`.`user_settings` SELECT * FROM %s", stagingTableID.FullyQualifiedName()), parts[2])
	}
	{
		// Dedupe with composite keys + `__artie_updated_at` flag.
		tableID := NewTableIdentifier("project123", "public", "user_settings")
		stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

		parts := b.store.generateDedupeQueries(tableID, stagingTableID, []string{"user_id", "settings"}, kafkalib.TopicConfig{IncludeArtieUpdatedAt: true})
		assert.Len(b.T(), parts, 3)
		assert.Equal(
			b.T(),
			fmt.Sprintf("CREATE OR REPLACE TABLE %s OPTIONS (expiration_timestamp = TIMESTAMP(%s)) AS (SELECT * FROM `project123`.`public`.`user_settings` QUALIFY ROW_NUMBER() OVER (PARTITION BY `user_id`, `settings` ORDER BY `user_id` ASC, `settings` ASC, `__artie_updated_at` ASC) = 2)",
				stagingTableID.FullyQualifiedName(),
				fmt.Sprintf(`"%s"`, typing.ExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL))),
			),
			parts[0],
		)
		assert.Equal(b.T(), fmt.Sprintf("DELETE FROM `project123`.`public`.`user_settings` t1 WHERE EXISTS (SELECT * FROM %s t2 WHERE t1.`user_id` = t2.`user_id` AND t1.`settings` = t2.`settings`)", stagingTableID.FullyQualifiedName()), parts[1])
		assert.Equal(b.T(), fmt.Sprintf("INSERT INTO `project123`.`public`.`user_settings` SELECT * FROM %s", stagingTableID.FullyQualifiedName()), parts[2])
	}
}
