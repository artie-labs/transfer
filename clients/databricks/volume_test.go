package databricks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewVolume(t *testing.T) {
	{
		// Invalid
		{
			// Missing name
			_, err := NewVolume(map[string]any{"path": "path"})
			assert.ErrorContains(t, err, "volume name is missing")
		}
		{
			// Name isn't string
			_, err := NewVolume(map[string]any{"name": 1, "path": "path"})
			assert.ErrorContains(t, err, "volume name is not a string")
		}
		{
			// Missing path
			_, err := NewVolume(map[string]any{"name": "name"})
			assert.ErrorContains(t, err, "volume path is missing")
		}
		{
			// Path isn't string
			_, err := NewVolume(map[string]any{"name": "name", "path": 1})
			assert.ErrorContains(t, err, "volume path is not a string")
		}
	}
	{
		// Valid
		volume, err := NewVolume(map[string]any{"name": "name", "path": "path"})
		assert.Nil(t, err)
		assert.Equal(t, "name", volume.name)
		assert.Equal(t, "path", volume.path)
	}
}
