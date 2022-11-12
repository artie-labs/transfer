package postgres

import (
	"context"
	"fmt"
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
