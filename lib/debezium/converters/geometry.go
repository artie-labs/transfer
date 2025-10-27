package converters

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/geojson"

	"github.com/artie-labs/transfer/lib/typing"
)

type GeoJSON struct {
	Type       GeoJSONType    `json:"type"`
	Geometry   GeometryJSON   `json:"geometry"`
	Properties map[string]any `json:"properties,omitempty"`
}

type GeoJSONType string

const FeatureType GeoJSONType = "Feature"

type GeometryJSON struct {
	Type        GeometricShapes `json:"type"`
	Coordinates any             `json:"coordinates"`
}

type GeometricShapes string

const Point GeometricShapes = "Point"

type GeometryPoint struct{}

func (GeometryPoint) ToKindDetails() typing.KindDetails {
	return typing.Struct
}

// Convert takes in a map[string]any and returns a GeoJSON string. This function does not use WKB or SRID and leverages X, Y.
// https://debezium.io/documentation/reference/stable/connectors/postgresql.html#:~:text=io.debezium.data.geometry.Point
func (GeometryPoint) Convert(value any) (any, error) {
	valMap, ok := value.(map[string]any)
	if !ok {
		return "", fmt.Errorf("value is not map[string]any type")
	}

	x, ok := valMap["x"]
	if !ok {
		return "", fmt.Errorf("x coordinate does not exist")
	}

	y, ok := valMap["y"]
	if !ok {
		return "", fmt.Errorf("y coordinate does not exist")
	}

	geoJSON := GeoJSON{
		Type: FeatureType,
		Geometry: GeometryJSON{
			Type:        Point,
			Coordinates: []any{x, y},
		},
	}

	bytes, err := json.Marshal(geoJSON)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

type Geometry struct{}

func (Geometry) ToKindDetails() typing.KindDetails {
	// We will return this in GeoJSON format.
	return typing.Struct
}

func (Geometry) Convert(value any) (any, error) {
	valMap, ok := value.(map[string]any)
	if !ok {
		return "", fmt.Errorf("value is not map[string]any type")
	}

	wkbVal, ok := valMap["wkb"]
	if !ok {
		return "", fmt.Errorf("wkb does not exist")
	}

	wkbBytes, err := base64.StdEncoding.DecodeString(fmt.Sprint(wkbVal))
	if err != nil {
		return "", fmt.Errorf("error decoding base64: %w", err)
	}

	geom, err := ewkb.Unmarshal(wkbBytes)
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
