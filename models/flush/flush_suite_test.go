package flush

import (
	"context"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/dwh/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
	"github.com/stretchr/testify/suite"
	"testing"
)

type FlushTestSuite struct {
	suite.Suite
	fakeStore    *mocks.FakeStore
	fakeConsumer *mocks.FakeConsumer

	ctx context.Context
}

func (f *FlushTestSuite) SetupTest() {
	f.fakeStore = &mocks.FakeStore{}
	store := db.Store(f.fakeStore)

	f.ctx = context.Background()

	f.ctx = config.InjectSettingsIntoContext(f.ctx, &config.Settings{
		Config: &config.Config{
			Output: "snowflake",
		},
		VerboseLogging: false,
	})

	f.ctx = utils.InjectDwhIntoCtx(utils.DataWarehouse(f.ctx, &store), f.ctx)

	models.LoadMemoryDB()

	f.fakeConsumer = &mocks.FakeConsumer{}
	consumer.SetKafkaConsumer(map[string]kafkalib.Consumer{"foo": f.fakeConsumer})
}

func TestFlushTestSuite(t *testing.T) {
	suite.Run(t, new(FlushTestSuite))
}
