package snowflake

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func (s *SnowflakeTestSuite) TestSweep() {
	// This is a no-op, since store isn't a store w/ stages.
	assert.NoError(s.T(), s.store.Sweep(s.ctx))

	s.ctx = config.InjectSettingsIntoContext(s.ctx, &config.Settings{
		Config: &config.Config{
			Queue:                constants.Kafka,
			Output:               constants.SnowflakeStages,
			BufferRows:           5,
			FlushSizeKb:          50,
			FlushIntervalSeconds: 50,
			Kafka: &config.Kafka{
				GroupID:         "artie",
				BootstrapServer: "localhost:9092",
				TopicConfigs: []*kafkalib.TopicConfig{
					{

						Database:  "db",
						Schema:    "schema",
						TableName: "table1",
						Topic:     "topic1",
						CDCFormat: constants.DBZPostgresFormat,
					},
					{
						Database:  "db",
						Schema:    "schema",
						TableName: "table2",
						Topic:     "topic2",
						CDCFormat: constants.DBZPostgresFormat,
					},
				},
			},
		},
	})

	assert.NoError(s.T(), s.stageStore.Sweep(s.ctx))
	query, _ := s.fakeStageStore.QueryArgsForCall(0)
	assert.Equal(s.T(), `SELECT table_name, comment FROM db.information_schema.tables where table_name ILIKE '%__artie%' AND table_schema = UPPER('schema')`, query)
}
