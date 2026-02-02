package dialect

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIcebergDialect_IsColumnAlreadyExistsErr(t *testing.T) {
	_dialect := IcebergDialect{}
	assert.True(t, _dialect.IsColumnAlreadyExistsErr(fmt.Errorf("[FIELDS_ALREADY_EXISTS] Cannot add column, because `first_name` already exists")))
	assert.False(t, _dialect.IsColumnAlreadyExistsErr(nil))
}

func TestIcebergDialect_BuildDedupeQueries(t *testing.T) {
	_dialect := IcebergDialect{}
	tableID := NewTableIdentifier("{{catalog}}", "{{schema}}", "{{table}}")
	stagingTableID := NewTableIdentifier("{{catalog}}", "{{schema}}", "{{table_staging}}")

	{
		// Single primary key, __artie_updated_at = false
		primaryKeys := []string{"id"}
		queries := _dialect.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, false)
		assert.Equal(t, 4, len(queries))
		assert.Equal(t, "CREATE OR REPLACE TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY `id` ORDER BY `id` ASC ) AS __artie_rn FROM `{{catalog}}`.`{{schema}}`.`{{table}}` ) WHERE __artie_rn = 2", queries[0])
		assert.Equal(t, "ALTER TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` DROP COLUMN __artie_rn", queries[1])
		assert.Equal(t, "DELETE FROM `{{catalog}}`.`{{schema}}`.`{{table}}` t1 WHERE EXISTS (SELECT 1 FROM `{{catalog}}`.`{{schema}}`.`{{table_staging}}` t2 WHERE t1.`id` = t2.`id`)", queries[2])
		assert.Equal(t, "INSERT INTO `{{catalog}}`.`{{schema}}`.`{{table}}` SELECT * FROM `{{catalog}}`.`{{schema}}`.`{{table_staging}}`", queries[3])
	}

	{
		// Single primary key, __artie_updated_at = true
		primaryKeys := []string{"id"}
		queries := _dialect.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, true)
		assert.Equal(t, 4, len(queries))
		assert.Equal(t, "CREATE OR REPLACE TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY `id` ORDER BY `id` ASC, `__artie_updated_at` ASC ) AS __artie_rn FROM `{{catalog}}`.`{{schema}}`.`{{table}}` ) WHERE __artie_rn = 2", queries[0])
		assert.Equal(t, "ALTER TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` DROP COLUMN __artie_rn", queries[1])
		assert.Equal(t, "DELETE FROM `{{catalog}}`.`{{schema}}`.`{{table}}` t1 WHERE EXISTS (SELECT 1 FROM `{{catalog}}`.`{{schema}}`.`{{table_staging}}` t2 WHERE t1.`id` = t2.`id`)", queries[2])
		assert.Equal(t, "INSERT INTO `{{catalog}}`.`{{schema}}`.`{{table}}` SELECT * FROM `{{catalog}}`.`{{schema}}`.`{{table_staging}}`", queries[3])
	}

	{
		// Composite primary key, __artie_updated_at = false
		primaryKeys := []string{"user_id", "order_id"}
		queries := _dialect.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, false)
		assert.Equal(t, 4, len(queries))
		assert.Equal(t, "CREATE OR REPLACE TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY `user_id`, `order_id` ORDER BY `user_id` ASC, `order_id` ASC ) AS __artie_rn FROM `{{catalog}}`.`{{schema}}`.`{{table}}` ) WHERE __artie_rn = 2", queries[0])
		assert.Equal(t, "ALTER TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` DROP COLUMN __artie_rn", queries[1])
		assert.Equal(t, "DELETE FROM `{{catalog}}`.`{{schema}}`.`{{table}}` t1 WHERE EXISTS (SELECT 1 FROM `{{catalog}}`.`{{schema}}`.`{{table_staging}}` t2 WHERE t1.`user_id` = t2.`user_id` AND t1.`order_id` = t2.`order_id`)", queries[2])
		assert.Equal(t, "INSERT INTO `{{catalog}}`.`{{schema}}`.`{{table}}` SELECT * FROM `{{catalog}}`.`{{schema}}`.`{{table_staging}}`", queries[3])
	}

	{
		// Composite primary key, __artie_updated_at = true
		primaryKeys := []string{"user_id", "order_id"}
		queries := _dialect.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, true)
		assert.Equal(t, 4, len(queries))
		assert.Equal(t, "CREATE OR REPLACE TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY `user_id`, `order_id` ORDER BY `user_id` ASC, `order_id` ASC, `__artie_updated_at` ASC ) AS __artie_rn FROM `{{catalog}}`.`{{schema}}`.`{{table}}` ) WHERE __artie_rn = 2", queries[0])
		assert.Equal(t, "ALTER TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` DROP COLUMN __artie_rn", queries[1])
		assert.Equal(t, "DELETE FROM `{{catalog}}`.`{{schema}}`.`{{table}}` t1 WHERE EXISTS (SELECT 1 FROM `{{catalog}}`.`{{schema}}`.`{{table_staging}}` t2 WHERE t1.`user_id` = t2.`user_id` AND t1.`order_id` = t2.`order_id`)", queries[2])
		assert.Equal(t, "INSERT INTO `{{catalog}}`.`{{schema}}`.`{{table}}` SELECT * FROM `{{catalog}}`.`{{schema}}`.`{{table_staging}}`", queries[3])
	}
}

func TestIcebergDialect_BuildCreateTemporaryView(t *testing.T) {
	query := IcebergDialect{}.BuildCreateTemporaryView("{{VIEW_NAME}}", []string{"{{ID}}", "{{NAME}}"}, "{{S3_PATH}}")
	assert.Equal(t, `CREATE OR REPLACE TEMPORARY VIEW {{VIEW_NAME}} ( {{ID}}, {{NAME}} ) USING csv OPTIONS (path '{{S3_PATH}}', sep '\t', header 'false', compression 'gzip', nullValue '__artie_null_value', escape '"', inferSchema 'false', multiLine 'true', lineSep '\n');`, query)
}
