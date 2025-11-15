package dialect

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func TestBuildCreateStageQuery(t *testing.T) {
	{
		// No prefix or credentials
		query := SnowflakeDialect{}.BuildCreateStageQuery("db", "schema", "stage", "bucket", "", "")
		assert.Equal(t, query, fmt.Sprintf(`CREATE OR REPLACE STAGE db.schema.stage URL = 's3://bucket' FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='%s' EMPTY_FIELD_AS_NULL=FALSE)`, constants.NullValuePlaceholder))
	}
	{
		// With prefix and credentials
		query := SnowflakeDialect{}.BuildCreateStageQuery("db", "schema", "stage", "bucket", "prefix", "AWS_KEY_ID = 'key' AWS_SECRET_KEY = 'secret'")
		assert.Equal(t, query, fmt.Sprintf(`CREATE OR REPLACE STAGE db.schema.stage URL = 's3://bucket/prefix' FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='%s' EMPTY_FIELD_AS_NULL=FALSE) CREDENTIALS = ( AWS_KEY_ID = 'key' AWS_SECRET_KEY = 'secret' )`, constants.NullValuePlaceholder))
	}
}

func TestBuildCreatePipeQuery(t *testing.T) {
	{
		// Single column
		query := SnowflakeDialect{}.BuildCreatePipeQuery(NewTableIdentifier("db", "schema", "my_pipe"), NewTableIdentifier("db", "schema", "my_table"), []string{"id"})
		assert.Equal(t, `CREATE OR REPLACE PIPE db.schema.my_pipe AS COPY INTO db.schema.my_table FROM (SELECT $1:"ID" FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')))`, query)
	}
	{
		// Multiple columns
		query := SnowflakeDialect{}.BuildCreatePipeQuery(NewTableIdentifier("db", "schema", "my_pipe"), NewTableIdentifier("db", "schema", "my_table"), []string{"id", "name", "timestamp"})
		assert.Equal(t, `CREATE OR REPLACE PIPE db.schema.my_pipe AS COPY INTO db.schema.my_table FROM (SELECT $1:"ID", $1:"NAME", $1:"TIMESTAMP" FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')))`, query)
	}
	{
		// Column with special characters (should be quoted)
		query := SnowflakeDialect{}.BuildCreatePipeQuery(NewTableIdentifier("db", "schema", "my_pipe"), NewTableIdentifier("db", "schema", "my_table"), []string{"user_id", "created_at"})
		assert.Equal(t, `CREATE OR REPLACE PIPE db.schema.my_pipe AS COPY INTO db.schema.my_table FROM (SELECT $1:"USER_ID", $1:"CREATED_AT" FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')))`, query)
	}
}
