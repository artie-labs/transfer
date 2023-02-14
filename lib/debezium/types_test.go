package debezium

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFromDebeziumTypeToTime(t *testing.T) {
	dt, err := FromDebeziumTypeToTime(Date, int64(19401))
	assert.Equal(t, "2023-02-13", dt.String(""))
	assert.NoError(t, err)
}
