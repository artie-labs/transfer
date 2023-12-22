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
}

func (e *EventsTestSuite) SetupTest() {
	e.ctx = context.Background()
	e.ctx = config.InjectSettingsIntoContext(e.ctx, &config.Settings{
		Config: &config.Config{
			FlushIntervalSeconds: 10,
			FlushSizeKb:          1024,
			BufferRows:           1000,
		},
	})

	e.ctx = models.LoadMemoryDB(e.ctx)
}

func TestEventsTestSuite(t *testing.T) {
	suite.Run(t, new(EventsTestSuite))
}
