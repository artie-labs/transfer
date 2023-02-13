package debezium

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFromDebeziumTypeToTime(t *testing.T) {
	dt := FromDebeziumTypeToTime(Date, int64(19401))
	assert.Equal(t, "2023-02-13 00:00:00 +0000 UTC", dt.String())
}
