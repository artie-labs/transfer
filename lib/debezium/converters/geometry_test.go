package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseGeometryPoint(t *testing.T) {
	{
		geoJSON, err := GeometryPoint{}.Convert(map[string]any{
			"x":    2.2945,
			"y":    48.8584,
			"wkb":  "AQEAAABCYOXQIlsCQHZxGw3gbUhA",
			"srid": nil,
		})

		geoJSONString, ok := geoJSON.(string)
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.Equal(t, `{"type":"Feature","geometry":{"type":"Point","coordinates":[2.2945,48.8584]}}`, geoJSONString)
	}
}

func TestGeometryWkb(t *testing.T) {
	{
		geoJSONString, err := Geometry{}.Convert(map[string]any{
			"wkb":  "AQEAAAAAAAAAAADwPwAAAAAAAPA/",
			"srid": nil,
		})

		assert.NoError(t, err)
		assert.Equal(t, `{"type":"Feature","geometry":{"type":"Point","coordinates":[1,1]},"properties":null}`, geoJSONString)
	}
}

func TestLine(t *testing.T) {
	{
		// Test valid line with float64 values
		result, err := Line{}.Convert([]any{1.0, 2.0, 3.0})
		assert.NoError(t, err)
		assert.Equal(t, []any{1.0, 2.0, 3.0}, result)
	}
	{
		// Test valid line with mixed numeric types
		result, err := Line{}.Convert([]any{int(1), float32(2.5), int64(3)})
		assert.NoError(t, err)
		assert.Equal(t, []any{1.0, 2.5, 3.0}, result)
	}
	{
		// Test invalid input - not an array
		_, err := Line{}.Convert("not an array")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not []any type")
	}
	{
		// Test invalid input - wrong array length
		_, err := Line{}.Convert([]any{1.0, 2.0})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected array of length 3")
	}
	{
		// Test invalid input - wrong type in array
		_, err := Line{}.Convert([]any{1.0, "invalid", 3.0})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected type")
	}
}
