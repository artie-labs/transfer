package mongo

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

// JSONEToMap will take JSONE data in bytes, parse all the custom types
// Then from all the custom types,
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
	err := bson.UnmarshalExtJSON(val, false, &bsonDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal ext json: %w", err)
	}

	// Directly decode bsonDoc to map[string]any
	return bsonDocToMap(bsonDoc), nil
}

// Helper function to convert bson.D to map[string]any
func bsonDocToMap(doc bson.D) map[string]any {
	result := make(map[string]any)
	for _, elem := range doc {
		result[elem.Key] = bsonValueToGoValue(elem.Value)
	}
	return result
}

func bsonArrayToSlice(arr bson.A) []any {
	result := make([]any, len(arr))
	for i, elem := range arr {
		result[i] = bsonValueToGoValue(elem)
	}
	return result
}

func bsonBinaryValueToMap(value primitive.Binary) (any, error) {
	action := "$binary"

	switch value.Subtype {
	case
		bson.TypeBinaryUUIDOld,
		bson.TypeBinaryUUID:
		parsedUUID, err := uuid.FromBytes(value.Data)
		if err != nil {
			return nil, err
		}

		return parsedUUID.String(), nil
	}

	//switch value.Subtype {
	//case bson.TypeBinaryGeneric, bson.TypeBinaryFunction, bson.TypeBinaryMD5, bson.TypeBinaryEncrypted:
	//
	//case bson.TypeBinaryUUIDOld, bson.TypeBinaryUUID:
	//}

	return map[string]any{
		action: map[string]any{
			"base64":  base64.StdEncoding.EncodeToString(value.Data),
			"subType": fmt.Sprintf("%02x", value.Subtype),
		},
	}, nil
}

// bsonValueToGoValue - https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
func bsonValueToGoValue(value any) any {
	switch v := value.(type) {
	case primitive.DateTime:
		return time.Unix(0, int64(v)*int64(time.Millisecond)).UTC().Format(ext.ISO8601)
	case primitive.ObjectID:
		return v.Hex()
	case primitive.Binary:
		return bsonBinaryValueToMap(v)
	case primitive.Decimal128:
		if parsedFloat, err := strconv.ParseFloat(v.String(), 64); err == nil {
			return parsedFloat
		}
		return v.String()
	case primitive.Timestamp:
		return time.Unix(int64(v.T), 0).UTC().Format(ext.ISO8601)
	case bson.D:
		return bsonDocToMap(v)
	case bson.A:
		return bsonArrayToSlice(v)
	case primitive.MaxKey:
		return map[string]any{"$maxKey": 1}
	case primitive.MinKey:
		return map[string]any{"$minKey": 1}
	case primitive.JavaScript:
		return map[string]any{"$code": v}
	default:
		fmt.Println("v", v, fmt.Sprintf("type: %T", v))
		return v
	}
}
