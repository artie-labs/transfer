package postgres

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"time"

	"github.com/stretchr/testify/assert"
)

func (p *PostgresTestSuite) TestGetPrimaryKey() {
	valString := `{"id": 47}`
	pkName, pkVal, err := p.GetPrimaryKey(context.Background(), []byte(valString))
	assert.Equal(p.T(), pkName, "id")
	assert.Equal(p.T(), fmt.Sprint(pkVal), fmt.Sprint(47)) // Don't have to deal with float and int conversion
	assert.Equal(p.T(), err, nil)
}

func (p *PostgresTestSuite) TestGetPrimaryKeyUUID() {
	valString := `{"uuid": "ca0cefe9-45cf-44fa-a2ab-ec5e7e5522a3"}`
	pkName, pkVal, err := p.GetPrimaryKey(context.Background(), []byte(valString))
	assert.Equal(p.T(), pkName, "uuid")
	assert.Equal(p.T(), fmt.Sprint(pkVal), "ca0cefe9-45cf-44fa-a2ab-ec5e7e5522a3")
	assert.Equal(p.T(), err, nil)
}

func (p *PostgresTestSuite) TestSource_GetExecutionTime() {
	source := Source{
		Connector: "postgresql",
		TsMs:      1665458364942, // Tue Oct 11 2022 03:19:24
	}

	event := &Event{Source: source}
	assert.Equal(p.T(), time.Date(2022, time.October,
		11, 3, 19, 24, 942000000, time.UTC), event.GetExecutionTime())
}

func (p *PostgresTestSuite) TestGetDataTestInsert() {
	after := map[string]interface{}{
		"pk":           1,
		"foo":          "bar",
		"name":         "dusty",
		"favoriteFood": "jerky",
	}

	var tc kafkalib.TopicConfig

	evt := Event{
		Before:    nil,
		After:     after,
		Operation: "c",
	}

	evtData := evt.GetData("pk", 1, tc)
	assert.Equal(p.T(), len(after)+1, len(evtData), "has deletion flag")

	deletionFlag, isOk := evtData[config.DeleteColumnMarker]
	assert.True(p.T(), isOk)
	assert.False(p.T(), deletionFlag.(bool))

	delete(evtData, config.DeleteColumnMarker)
	assert.Equal(p.T(), after, evtData)
}

func (p *PostgresTestSuite) TestGetDataTestDelete() {
	tc := kafkalib.TopicConfig{
		IdempotentKey: "updated_at",
	}

	now := time.Now().UTC()
	evt := Event{
		Before:    nil,
		After:     nil,
		Operation: "c",
		Source:    Source{TsMs: now.UnixMilli()},
	}

	evtData := evt.GetData("pk", 1, tc)
	shouldDelete, isOk := evtData[config.DeleteColumnMarker]
	assert.True(p.T(), isOk)
	assert.True(p.T(), shouldDelete.(bool))

	assert.Equal(p.T(), 3, len(evtData), evtData)
	assert.Equal(p.T(), evtData["pk"], 1)
	assert.Equal(p.T(), evtData[tc.IdempotentKey], now.Format(time.RFC3339))
}

func (p *PostgresTestSuite) TestGetDataTestUpdate() {
	before := map[string]interface{}{
		"pk":           1,
		"foo":          "bar",
		"name":         "dusty",
		"favoriteFood": "apples",
		"age":          1,
		"weight_lbs":   25,
	}

	after := map[string]interface{}{
		"pk":           1,
		"foo":          "bar",
		"name":         "dusty",
		"favoriteFood": "jerky",
		"age":          2,
		"weight_lbs":   33,
	}

	var tc kafkalib.TopicConfig
	evt := Event{
		Before:    before,
		After:     after,
		Operation: "c",
	}

	evtData := evt.GetData("pk", 1, tc)
	assert.Equal(p.T(), len(after)+1, len(evtData), "has deletion flag")

	deletionFlag, isOk := evtData[config.DeleteColumnMarker]
	assert.True(p.T(), isOk)
	assert.False(p.T(), deletionFlag.(bool))

	delete(evtData, config.DeleteColumnMarker)
	assert.Equal(p.T(), after, evtData)
}
