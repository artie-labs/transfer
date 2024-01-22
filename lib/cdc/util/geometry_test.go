package util

import "github.com/stretchr/testify/assert"

func (u *UtilTestSuite) TestParseGeometryPoint() {
	{
		geoJSONString, err := parseGeometryPoint(map[string]interface{}{
			"x":    2.2945,
			"y":    48.8584,
			"wkb":  "AQEAAABCYOXQIlsCQHZxGw3gbUhA",
			"srid": nil,
		})

		assert.NoError(u.T(), err)
		assert.Equal(u.T(), `{"type":"Feature","geometry":{"type":"Point","coordinates":[2.2945,48.8584]}}`, geoJSONString)
	}
}
