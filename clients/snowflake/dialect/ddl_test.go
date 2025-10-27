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
