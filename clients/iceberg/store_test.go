package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
)

func TestMerge_MSM_SkipUpdate(t *testing.T) {
	// When MSM is enabled but there's no data, Merge should exit early with (false, nil).
	topicConfig := kafkalib.TopicConfig{
		Database:  "db",
		Schema:    "schema",
		TableName: "table",
		MultiStepMergeSettings: &kafkalib.MultiStepMergeSettings{
			Enabled:    true,
			FlushCount: 3,
		},
	}

	tableData := optimization.NewTableData(nil, config.Replication, []string{"id"}, topicConfig, "table")
	assert.True(t, tableData.ShouldSkipUpdate())
	assert.True(t, tableData.MultiStepMergeSettings().Enabled)

	store := Store{}
	commitTx, err := store.Merge(t.Context(), tableData, nil)
	assert.NoError(t, err)
	assert.False(t, commitTx)
}

func TestMerge_NonMSM_SkipUpdate(t *testing.T) {
	// When MSM is not enabled and there's no data, Merge should exit early with (false, nil).
	topicConfig := kafkalib.TopicConfig{
		Database:  "db",
		Schema:    "schema",
		TableName: "table",
	}

	tableData := optimization.NewTableData(nil, config.Replication, []string{"id"}, topicConfig, "table")
	assert.True(t, tableData.ShouldSkipUpdate())
	assert.False(t, tableData.MultiStepMergeSettings().Enabled)

	store := Store{}
	commitTx, err := store.Merge(t.Context(), tableData, nil)
	assert.NoError(t, err)
	assert.False(t, commitTx)
}
