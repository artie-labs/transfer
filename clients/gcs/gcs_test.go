package gcs

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
	td := &optimization.TableData{}
	td.SetLatestTimestamp(ts)

	fp := buildTemporaryFilePath(td)
	assert.True(t, strings.HasPrefix(fp, "/tmp/1577836800000_"), fp)
	assert.True(t, strings.HasSuffix(fp, ".parquet"), fp)
}

func TestObjectPrefix(t *testing.T) {
	td := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", TableName: "table", Schema: "public"}, "table")
	{
		// Valid - No Folder
		store := &Store{
			config: config.Config{
				GCS: &config.GCSSettings{
					Bucket:       "bucket",
					ProjectID:    "project",
					OutputFormat: constants.ParquetFormat,
				},
			},
		}

		assert.Equal(t, fmt.Sprintf("db.public.table/date=%s", time.Now().Format(time.DateOnly)), store.ObjectPrefix(td))
	}
	{
		// Valid - With Folder
		store := &Store{
			config: config.Config{
				GCS: &config.GCSSettings{
					Bucket:       "bucket",
					ProjectID:    "project",
					FolderName:   "foo",
					OutputFormat: constants.ParquetFormat,
				},
			},
		}

		assert.Equal(t, fmt.Sprintf("foo/db.public.table/date=%s", time.Now().Format(time.DateOnly)), store.ObjectPrefix(td))
	}
}
