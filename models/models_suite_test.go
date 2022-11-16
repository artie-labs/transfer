package models

import (
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
	"testing"
)

type ModelsTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
}

func (m *ModelsTestSuite) SetupTest() {
	LoadMemoryDB()
}

func TestModelsTestSuite(t *testing.T) {
	suite.Run(t, new(ModelsTestSuite))
}
