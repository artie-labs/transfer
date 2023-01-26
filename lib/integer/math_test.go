package integer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMax(t *testing.T) {
	assert.Equal(t, Max(), -1)
	assert.Equal(t, Max(1, 2, 3), 3)
	assert.Equal(t, Max(1, 2, 3, 3, 3, 3), 3)
	assert.Equal(t, Max(3, 3, 3, 3, 3, 3), 3)

	assert.Equal(t, Max(-999, -99, -9), -9)
	assert.Equal(t, Max(999, 99, 9), 999)
	assert.Equal(t, Max(999, 99, 9999), 9999)
}
