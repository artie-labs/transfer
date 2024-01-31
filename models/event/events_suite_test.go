package event

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/models"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/suite"
)

type EventsTestSuite struct {
	suite.Suite
	ctx context.Context
	cfg config.Config
}

func (e *EventsTestSuite) SetupTest() {
	e.cfg = config.Config{
		FlushIntervalSeconds: 10,
		FlushSizeKb:          1024,
		BufferRows:           1000,
	}
	e.ctx = models.LoadMemoryDB(context.Background())
}

func TestEventsTestSuite(t *testing.T) {
	suite.Run(t, new(EventsTestSuite))
}
