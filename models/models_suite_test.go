package models

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ModelsTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (m *ModelsTestSuite) SetupTest() {
	m.ctx = LoadMemoryDB(context.Background())
}

func TestModelsTestSuite(t *testing.T) {
	suite.Run(t, new(ModelsTestSuite))
}
