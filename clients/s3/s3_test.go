package s3

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
)

func TestBuildTemporaryFilePath(t *testing.T) {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	fp := buildTemporaryFilePath(&optimization.TableData{LatestCDCTs: ts})
	assert.True(t, strings.HasPrefix(fp, "/tmp/1577836800000_"), fp)
	assert.True(t, strings.HasSuffix(fp, ".parquet"), fp)
}

func TestObjectPrefix(t *testing.T) {
	td := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", TableName: "table", Schema: "public"}, "table")
	{
		// Valid - No Folder
		store, err := LoadStore(t.Context(), config.Config{S3: &config.S3Settings{
			Bucket:             "bucket",
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
			AwsRegion:          "us-east-1",
			OutputFormat:       constants.ParquetFormat,
		}})

		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("db.public.table/date=%s", time.Now().Format(time.DateOnly)), store.ObjectPrefix(td))
	}
	{
		// Valid - With Folder
		store, err := LoadStore(t.Context(), config.Config{S3: &config.S3Settings{
			Bucket:             "bucket",
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
			AwsRegion:          "us-east-1",
			FolderName:         "foo",
			OutputFormat:       constants.ParquetFormat,
		}})

		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("foo/db.public.table/date=%s", time.Now().Format(time.DateOnly)), store.ObjectPrefix(td))
	}
}
