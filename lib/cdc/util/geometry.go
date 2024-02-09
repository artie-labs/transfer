package util

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/twpayne/go-geom/encoding/geojson"

	"github.com/twpayne/go-geom/encoding/wkb"
)

type GeoJSON struct {
	Type       GeoJSONType            `json:"type"`
	Geometry   Geometry               `json:"geometry"`
	Properties map[string]interface{} `json:"properties,omitempty"`
}

type GeoJSONType string

const FeatureType GeoJSONType = "Feature"

type Geometry struct {
	Type        GeometricShapes `json:"type"`
	Coordinates interface{}     `json:"coordinates"`
}

type GeometricShapes string

const Point GeometricShapes = "Point"

// ParseGeometry takes in a map[string]interface{} and returns a GeoJSON string.
// This function does not use WKB or SRID and leverages X, Y.
// https://debezium.io/documentation/reference/stable/connectors/postgresql.html#:~:text=io.debezium.data.geometry.Point
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
		Type: FeatureType,
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

// parseGeometry will take in an object with b64 encoded wkb and return a GeoJSON string.
func parseGeometry(value interface{}) (string, error) {
	valMap, isOk := value.(map[string]interface{})
	if !isOk {
		return "", fmt.Errorf("value is not map[string]interface{} type")
	}

	wkbVal, isOk := valMap["wkb"]
	if !isOk {
		return "", fmt.Errorf("wkb does not exist")
	}

	wkbBytes, err := base64.StdEncoding.DecodeString(fmt.Sprint(wkbVal))
	if err != nil {
		return "", fmt.Errorf("error decoding base64: %w", err)
	}

	geom, err := wkb.Unmarshal(wkbBytes)
	if err != nil {
		return "", fmt.Errorf("error unmarshalling WKB bytes: %w", err)
	}

	feature := geojson.Feature{
		Geometry: geom,
	}

	bytes, err := feature.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("error marshalling GeoJSON: %w", err)
	}

	return string(bytes), nil
}
