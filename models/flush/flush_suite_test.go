package flush

import (
	"context"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/kafka"
	"github.com/stretchr/testify/suite"
	"testing"
)

type FlushTestSuite struct {
	suite.Suite
	fakeStore    *mocks.FakeStore
	fakeConsumer *mocks.FakeConsumer
}

func (f *FlushTestSuite) SetupTest() {
	f.fakeStore = &mocks.FakeStore{}
	store := db.Store(f.fakeStore)

	ctx := context.Background()
	snowflake.LoadSnowflake(ctx, &store)
	models.LoadMemoryDB()

	f.fakeConsumer = &mocks.FakeConsumer{}
	kafka.SetKafkaConsumer(map[string]kafkalib.Consumer{"foo": f.fakeConsumer})
}

func TestFlushTestSuite(t *testing.T) {
	suite.Run(t, new(FlushTestSuite))
}
