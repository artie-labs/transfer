package snowflake

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func (s *SnowflakeTestSuite) TestSweep() {
	tcs := []*kafkalib.TopicConfig{
		{

			Database:     "db",
			Schema:       "schema",
			TableName:    "table1",
			Topic:        "topic1",
			CDCFormat:    constants.DBZPostgresFormat,
			CDCKeyFormat: kafkalib.JSONKeyFmt,
		},
		{
			Database:     "db",
			Schema:       "schema",
			TableName:    "table2",
			Topic:        "topic2",
			CDCFormat:    constants.DBZPostgresFormat,
			CDCKeyFormat: kafkalib.JSONKeyFmt,
		},
	}

	for _, tc := range tcs {
		tc.Load()
	}

	s.stageStore.config = config.Config{
		Queue:                constants.Kafka,
		Output:               constants.Snowflake,
		BufferRows:           5,
		FlushSizeKb:          50,
		FlushIntervalSeconds: 50,
		Kafka: &config.Kafka{
			GroupID:         "artie",
			BootstrapServer: "localhost:9092",
			TopicConfigs:    tcs,
		},
	}

	assert.NoError(s.T(), s.stageStore.Sweep())
	query, _ := s.fakeStageStore.QueryArgsForCall(0)
	assert.Equal(s.T(), `SELECT table_name, IFNULL(comment, '') FROM db.information_schema.tables where table_name ILIKE '%__artie%' AND table_schema = UPPER('schema')`, query)
}
