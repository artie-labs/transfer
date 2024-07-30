package event

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/mocks"

	"github.com/artie-labs/transfer/models"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/suite"
)

type EventsTestSuite struct {
	suite.Suite
	cfg       config.Config
	db        *models.DatabaseData
	fakeEvent *mocks.FakeEvent
}

func (e *EventsTestSuite) SetupTest() {
	e.cfg = config.Config{
		FlushIntervalSeconds: 10,
		FlushSizeKb:          1024,
		BufferRows:           1000,
	}
	e.db = models.NewMemoryDB()

	fakeEvent := &mocks.FakeEvent{}
	fakeEvent.GetDataReturns(map[string]any{constants.DeleteColumnMarker: false, constants.OnlySetDeleteColumnMarker: false}, nil)
	fakeEvent.GetColumnsReturns(&columns.Columns{}, nil)
	fakeEvent.GetTableNameReturns("foo")
	e.fakeEvent = fakeEvent
}

func TestEventsTestSuite(t *testing.T) {
	suite.Run(t, new(EventsTestSuite))
}
