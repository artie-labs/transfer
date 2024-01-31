package event

import (
	"testing"

	"github.com/artie-labs/transfer/models"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/suite"
)

type EventsTestSuite struct {
	suite.Suite
	cfg config.Config
	db  *models.DatabaseData
}

func (e *EventsTestSuite) SetupTest() {
	e.cfg = config.Config{
		FlushIntervalSeconds: 10,
		FlushSizeKb:          1024,
		BufferRows:           1000,
	}
	e.db = models.NewMemoryDB()
}

func TestEventsTestSuite(t *testing.T) {
	suite.Run(t, new(EventsTestSuite))
}
