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
		// __artie_updated_at = false
		queries := _dialect.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, false)
		assert.Equal(t, 3, len(queries))
		assert.Equal(t, "CREATE OR REPLACE TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY `id` ORDER BY `id` DESC ) AS __artie_rn FROM `{{catalog}}`.`{{schema}}`.`{{table}}` ) WHERE __artie_rn = 1", queries[0])
		assert.Equal(t, "ALTER TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` DROP COLUMN __artie_rn", queries[1])
		assert.Equal(t, "INSERT OVERWRITE `{{catalog}}`.`{{schema}}`.`{{table}}` TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}`", queries[2])
	}

	{
		// __artie_updated_at = true
		queries := _dialect.BuildDedupeQueries(tableID, stagingTableID, primaryKeys, true)
		assert.Equal(t, 3, len(queries))
		assert.Equal(t, "CREATE OR REPLACE TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` AS SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY `id` ORDER BY `id` DESC, `__artie_updated_at` DESC ) AS __artie_rn FROM `{{catalog}}`.`{{schema}}`.`{{table}}` ) WHERE __artie_rn = 1", queries[0])
		assert.Equal(t, "ALTER TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}` DROP COLUMN __artie_rn", queries[1])
		assert.Equal(t, "INSERT OVERWRITE `{{catalog}}`.`{{schema}}`.`{{table}}` TABLE `{{catalog}}`.`{{schema}}`.`{{table_staging}}`", queries[2])
	}

}
