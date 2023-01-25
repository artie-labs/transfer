package snowflake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
)

type SnowflakeTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
	store     *SnowflakeStore
}

func (s *SnowflakeTestSuite) SetupTest() {
	ctx := context.Background()

	s.fakeStore = &mocks.FakeStore{}
	store := db.Store(s.fakeStore)
	s.store = LoadSnowflake(ctx, &store)

	mdConfig = &metadataConfig{
		snowflakeTableToConfig: make(map[string]*snowflakeTableConfig),
	}
}

func TestSnowflakeTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeTestSuite))
}
