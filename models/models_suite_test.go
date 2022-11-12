package models

import (
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
)

type ModelsTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
}

func (m *ModelsTestSuite) SetupTest() {
	LoadMemoryDB()
}
