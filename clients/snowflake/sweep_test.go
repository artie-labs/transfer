package snowflake

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (s *SnowflakeTestSuite) TestShouldDelete() {
	type _testCase struct {
		name         string
		comment      string
		expectDelete bool
	}
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)
	oneHourFromNow := now.Add(1 * time.Hour)
	testCases := []_testCase{
		{
			name:         "random",
			comment:      "random",
			expectDelete: false,
		},
		{
			name:         "one hour from now, but no expires: prefix",
			comment:      typing.ExpiresDate(oneHourFromNow),
			expectDelete: false,
		},
		{
			name:         "one hour ago, but no expires: prefix",
			comment:      typing.ExpiresDate(oneHourAgo),
			expectDelete: false,
		},
		{
			name:         "one hour ago, with prefix, but extra space",
			comment:      fmt.Sprintf("%s %s", constants.SnowflakeExpireCommentPrefix, typing.ExpiresDate(oneHourAgo)),
			expectDelete: false,
		},
		{
			name:         "one hour from now, with prefix, but extra space",
			comment:      fmt.Sprintf("%s %s", constants.SnowflakeExpireCommentPrefix, typing.ExpiresDate(oneHourFromNow)),
			expectDelete: false,
		},
		{
			name:         "one hour ago (expired)",
			comment:      fmt.Sprintf("%s%s", constants.SnowflakeExpireCommentPrefix, typing.ExpiresDate(oneHourAgo)),
			expectDelete: true,
		},
		{
			name:         "one hour from now (not yet expired)",
			comment:      fmt.Sprintf("%s%s", constants.SnowflakeExpireCommentPrefix, typing.ExpiresDate(oneHourFromNow)),
			expectDelete: false,
		},
	}

	for _, testCase := range testCases {
		actualShouldDelete := shouldDelete(testCase.comment)
		assert.Equal(s.T(), testCase.expectDelete, actualShouldDelete, testCase.name)
	}
}

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
	fmt.Println(s.fakeStageStore.QueryCallCount())
	query, _ := s.fakeStageStore.QueryArgsForCall(0)
	assert.Equal(s.T(), `SELECT table_name, comment FROM db.information_schema.tables where table_name ILIKE '%__artie%' AND table_schema = UPPER('schema')`, query)
}
