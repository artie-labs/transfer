package util

import (
	"encoding/json"
	"fmt"
)

// Note: Today, we are not doing anything about wkb and srid right now.

type GeoJSON struct {
	Type       GeoJSONType            `json:"type"`
	Geometry   Geometry               `json:"geometry"`
	Properties map[string]interface{} `json:"properties,omitempty"`
}

type GeoJSONType string

const Feature GeoJSONType = "Feature"

type Geometry struct {
	Type        GeometricShapes `json:"type"`
	Coordinates interface{}     `json:"coordinates"`
}

type GeometricShapes string

const Point GeometricShapes = "Point"

func parseGeometryPoint(value interface{}) (string, error) {
	valMap, isOk := value.(map[string]interface{})
	if !isOk {
		return "", fmt.Errorf("value is not map[string]interface{} type")
	}

	x, isOk := valMap["x"]
	if !isOk {
		return "", fmt.Errorf("x coordinate does not exist")
	}

	y, isOk := valMap["y"]
	if !isOk {
		return "", fmt.Errorf("y coordinate does not exist")
	}

	geoJSON := GeoJSON{
		Type: Feature,
		Geometry: Geometry{
			Type:        Point,
			Coordinates: []interface{}{x, y},
		},
	}

	bytes, err := json.Marshal(geoJSON)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}
