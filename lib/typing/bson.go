package typing

import (
	"encoding/base64"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// JSONEToMap - Takes a JSON Extended string and converts it to a map[string]any
func JSONEToMap(val []byte) (map[string]any, error) {
	// We are escaping `NaN`, `Infinity` and `-Infinity` (literal values)
	re := regexp.MustCompile(`\bNaN\b|"\bNaN\b"|-\bInfinity\b|"-\bInfinity\b"|\bInfinity\b|"\bInfinity\b"`)
	val = []byte(re.ReplaceAllStringFunc(string(val), func(match string) string {
		if strings.Contains(match, "\"") {
			return match
		}
		return "null"
	}))

	var bsonDoc bson.D
	if err := bson.UnmarshalExtJSON(val, false, &bsonDoc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ext json: %w", err)
	}

	return bsonDocToMap(bsonDoc)
}

func bsonDocToMap(doc bson.D) (map[string]any, error) {
	result := make(map[string]any)
	for _, elem := range doc {
		value, err := bsonValueToGoValue(elem.Value)
		if err != nil {
			return nil, err
		}

		result[elem.Key] = value
	}
	return result, nil
}

func bsonArrayToSlice(arr bson.A) ([]any, error) {
	result := make([]any, len(arr))
	for i, elem := range arr {
		value, err := bsonValueToGoValue(elem)
		if err != nil {
			return nil, err
		}

		result[i] = value
	}
	return result, nil
}

func bsonBinaryValueToMap(value primitive.Binary) (any, error) {
	switch value.Subtype {
	case
		bson.TypeBinaryUUIDOld,
		bson.TypeBinaryUUID:
		if len(value.Data) == 16 {
			// UUIDs must be 16 bytes
			parsedUUID, err := uuid.FromBytes(value.Data)
			if err != nil {
				return nil, err
			}

			return parsedUUID.String(), nil
		}

		fallthrough
	default:
		return map[string]any{
			"$binary": map[string]any{
				"base64":  base64.StdEncoding.EncodeToString(value.Data),
				"subType": fmt.Sprintf("%02x", value.Subtype),
			},
		}, nil
	}
}

func validTimestamp(ts time.Time) bool {
	if ts.Year() > 9999 {
		return false
	}

	if ts.Year() <= 0 {
		return false
	}

	return true
}

func bsonValueToGoValue(value any) (any, error) {
	switch v := value.(type) {
	case primitive.DateTime:
		dt := v.Time().UTC()
		if !validTimestamp(dt) {
			return nil, nil
		}

		return dt, nil
	case primitive.Timestamp:
		ts := time.Unix(int64(v.T), 0).UTC()
		if !validTimestamp(ts) {
			return nil, nil
		}

		return ts, nil
	case primitive.ObjectID:
		return v.Hex(), nil
	case primitive.Binary:
		return bsonBinaryValueToMap(v)
	case primitive.Decimal128:
		// We purposefully chose a string representation here because not all systems can correctly handle Decimal128 without losing precision
		return v.String(), nil
	case bson.D:
		return bsonDocToMap(v)
	case bson.A:
		return bsonArrayToSlice(v)
	case primitive.MaxKey:
		return map[string]any{"$maxKey": 1}, nil
	case primitive.MinKey:
		return map[string]any{"$minKey": 1}, nil
	case primitive.JavaScript:
		return map[string]any{"$code": string(v)}, nil
	case primitive.CodeWithScope:
		return map[string]any{"$code": string(v.Code), "$scope": v.Scope}, nil
	case
		nil,
		bool,
		string,
		int32, int64,
		float32:
		return v, nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, nil
		}

		return v, nil
	default:
		return nil, fmt.Errorf("unexpected type: %T, value: %v", v, v)
	}
}
