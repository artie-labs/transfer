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
