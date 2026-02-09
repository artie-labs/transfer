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

	primaryKeys := []string{"id"}

	{
		// __artie_updated_at = false, no tableColumns (fallback path)
		queries := _dialect.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, false, nil)
		assert.Equal(t, 3, len(queries))
		assert.Equal(t, "CREATE OR REPLACE TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY `id` ORDER BY `id` DESC ) AS __artie_rn FROM `{{catalog}}`.`{{schema}}`.`{{table}}` ) WHERE __artie_rn = 1", queries[0])
		assert.Equal(t, "ALTER TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` DROP COLUMN __artie_rn", queries[1])
		assert.Equal(t, "INSERT OVERWRITE `{{catalog}}`.`{{schema}}`.`{{table}}` TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}`", queries[2])
	}

	{
		// __artie_updated_at = true, no tableColumns (fallback path)
		queries := _dialect.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, true, nil)
		assert.Equal(t, 3, len(queries))
		assert.Equal(t, "CREATE OR REPLACE TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY `id` ORDER BY `id` DESC, `__artie_updated_at` DESC ) AS __artie_rn FROM `{{catalog}}`.`{{schema}}`.`{{table}}` ) WHERE __artie_rn = 1", queries[0])
		assert.Equal(t, "ALTER TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` DROP COLUMN __artie_rn", queries[1])
		assert.Equal(t, "INSERT OVERWRITE `{{catalog}}`.`{{schema}}`.`{{table}}` TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}`", queries[2])
	}

	{
		// Fast path: tableColumns provided → single INSERT OVERWRITE ... SELECT
		tableColumns := []string{"id", "name"}
		queries := _dialect.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, false, tableColumns)
		assert.Equal(t, 1, len(queries))
		assert.Contains(t, queries[0], "INSERT OVERWRITE `{{catalog}}`.`{{schema}}`.`{{table}}` SELECT sub.`id`, sub.`name` FROM ")
		assert.Contains(t, queries[0], "WHERE sub.__artie_rn = 1")
	}
}

func TestIcebergDialect_BuildCreateTemporaryView(t *testing.T) {
	query := IcebergDialect{}.BuildCreateTemporaryView("{{VIEW_NAME}}", []string{"{{ID}}", "{{NAME}}"}, "{{S3_PATH}}")
	assert.Equal(t, `CREATE OR REPLACE TEMPORARY VIEW {{VIEW_NAME}} ( {{ID}}, {{NAME}} ) USING csv OPTIONS (path '{{S3_PATH}}', sep '\t', header 'false', compression 'gzip', nullValue '__artie_null_value', escape '"', inferSchema 'false', multiLine 'true', lineSep '\n');`, query)
}
