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

type Line struct{}

func (Line) ToKindDetails() typing.KindDetails {
	return typing.Array
}

// Convert takes a value that represents a PostgreSQL line type {A, B, C} (coefficients of Ax + By + C = 0)
// and returns it as an array of float64 values.
func (Line) Convert(value any) (any, error) {
	// PostgreSQL line type comes through as an array of 3 numeric values: {A, B, C}
	// representing the linear equation Ax + By + C = 0
	arr, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("value is not []any type, got %T", value)
	}

	if len(arr) != 3 {
		return nil, fmt.Errorf("expected array of length 3 for line type, got %d", len(arr))
	}

	// Convert each element to float64
	result := make([]float64, 3)
	for i, v := range arr {
		switch val := v.(type) {
		case float64:
			result[i] = val
		case float32:
			result[i] = float64(val)
		case int:
			result[i] = float64(val)
		case int32:
			result[i] = float64(val)
		case int64:
			result[i] = float64(val)
		default:
			return nil, fmt.Errorf("unexpected type %T for line coefficient at index %d", v, i)
		}
	}

	// Return as []any to match the expected return type
	return []any{result[0], result[1], result[2]}, nil
}
