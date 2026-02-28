package dialect

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
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

func TestIcebergDialect_BuildCreateTemporaryView(t *testing.T) {
	query := IcebergDialect{}.BuildCreateTemporaryView("{{VIEW_NAME}}", []string{"{{ID}}", "{{NAME}}"}, "{{S3_PATH}}")
	assert.Equal(t, `CREATE OR REPLACE TEMPORARY VIEW {{VIEW_NAME}} ( {{ID}}, {{NAME}} ) USING csv OPTIONS (path '{{S3_PATH}}', sep '\t', header 'false', compression 'gzip', nullValue '__artie_null_value', escape '"', inferSchema 'false', multiLine 'true', lineSep '\n');`, query)
}

func TestIcebergDialect_BuildMergeQueryIntoStagingTable(t *testing.T) {
	_dialect := IcebergDialect{}
	tableID := NewTableIdentifier("catalog", "schema", "msm_table")

	{
		// Single primary key
		cols := []columns.Column{
			columns.NewColumn("id", typing.String),
			columns.NewColumn("name", typing.String),
			columns.NewColumn("updated_at", typing.TimestampNTZ),
		}

		statements := _dialect.BuildMergeQueryIntoStagingTable(
			tableID,
			"`temp_view`",
			[]columns.Column{columns.NewColumn("id", typing.String)},
			nil,
			cols,
		)

		assert.Len(t, statements, 1)
		assert.Equal(t, `MERGE INTO `+"`catalog`.`schema`.`msm_table`"+` AS tgt USING `+"`temp_view`"+` AS stg ON tgt.`+"`id`"+` = stg.`+"`id`"+`
WHEN MATCHED THEN UPDATE SET `+"`id`"+`=stg.`+"`id`"+`,`+"`name`"+`=stg.`+"`name`"+`,`+"`updated_at`"+`=stg.`+"`updated_at`"+`
WHEN NOT MATCHED THEN INSERT (`+"`id`"+`,`+"`name`"+`,`+"`updated_at`"+`) VALUES (stg.`+"`id`"+`,stg.`+"`name`"+`,stg.`+"`updated_at`"+`)`, statements[0])
	}
	{
		// Composite primary key with additional equality strings
		cols := []columns.Column{
			columns.NewColumn("id", typing.String),
			columns.NewColumn("org_id", typing.String),
			columns.NewColumn("value", typing.Float),
		}

		statements := _dialect.BuildMergeQueryIntoStagingTable(
			tableID,
			"`temp_view`",
			[]columns.Column{
				columns.NewColumn("id", typing.String),
				columns.NewColumn("org_id", typing.String),
			},
			[]string{"tgt.`partition_key` = stg.`partition_key`"},
			cols,
		)

		assert.Len(t, statements, 1)
		assert.Contains(t, statements[0], "tgt.`id` = stg.`id` AND tgt.`org_id` = stg.`org_id` AND tgt.`partition_key` = stg.`partition_key`")
		assert.Contains(t, statements[0], "WHEN MATCHED THEN UPDATE SET")
		assert.Contains(t, statements[0], "WHEN NOT MATCHED THEN INSERT")
	}
}
