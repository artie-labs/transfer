package snowflake

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/typing"
)

type SnowflakeTestSuite struct {
	suite.Suite
	mockDB     sqlmock.Sqlmock
	stageStore *Store
}

func (s *SnowflakeTestSuite) SetupTest() {
	s.ResetStore()
}

func (s *SnowflakeTestSuite) ResetStore() {
	_db, mock, err := sqlmock.New()
	assert.NoError(s.T(), err)

	s.mockDB = mock
	s.stageStore, err = LoadStore(s.T().Context(), config.Config{Snowflake: &config.Snowflake{}}, typing.ToPtr(db.NewStoreWrapper(_db)))
	assert.NoError(s.T(), err)
}

func TestSnowflakeTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeTestSuite))
}
