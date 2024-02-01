package consumer

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/models"
	"github.com/stretchr/testify/suite"
)

type FlushTestSuite struct {
	suite.Suite
	fakeStore    *mocks.FakeStore
	fakeConsumer *mocks.FakeConsumer
	cfg          config.Config
	db           *models.DatabaseData
	dwh          destination.DataWarehouse
}

func (f *FlushTestSuite) SetupTest() {
	f.fakeStore = &mocks.FakeStore{}
	store := db.Store(f.fakeStore)

	f.cfg = config.Config{
		Kafka: &config.Kafka{
			BootstrapServer: "foo",
			GroupID:         "bar",
			Username:        "user",
			Password:        "abc",
			TopicConfigs: []*kafkalib.TopicConfig{
				{
					Database:     "db",
					Schema:       "schema",
					Topic:        "topic",
					CDCFormat:    constants.DBZPostgresFormat,
					CDCKeyFormat: kafkalib.JSONKeyFmt,
				},
			},
		},
		Queue:                constants.Kafka,
		Output:               "snowflake",
		BufferRows:           500,
		FlushIntervalSeconds: 60,
		FlushSizeKb:          500,
	}

	f.dwh = utils.DataWarehouse(f.cfg, &store)

	f.db = models.NewMemoryDB()

	f.fakeConsumer = &mocks.FakeConsumer{}
	SetKafkaConsumer(map[string]kafkalib.Consumer{"foo": f.fakeConsumer})
}

func TestFlushTestSuite(t *testing.T) {
	suite.Run(t, new(FlushTestSuite))
}
