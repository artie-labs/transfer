package mongo

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

// JSONEToMap will take JSONE data in bytes, parse all the custom types
// Then from all the custom types,
func JSONEToMap(val []byte) (map[string]interface{}, error) {
	// We are escaping `NaN`, `Infinity` and `-Infinity` (literal values)
	re := regexp.MustCompile(`\bNaN\b|"\bNaN\b"|-\bInfinity\b|"-\bInfinity\b|\bInfinity\b|"\bInfinity\b"`)
	val = []byte(re.ReplaceAllStringFunc(string(val), func(match string) string {
		if strings.Contains(match, "\"") {
			return match
		}
		return "null"
	}))

	var jsonMap map[string]interface{}
	var bsonDoc bson.D
	err := bson.UnmarshalExtJSON(val, false, &bsonDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal ext json, err: %v", err)
	}

	bytes, err := bson.MarshalExtJSONWithRegistry(createCustomRegistry().Build(),
		bsonDoc, false, true)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(bytes, &jsonMap)
	return jsonMap, err
}

var (
	tDateTime  = reflect.TypeOf(primitive.DateTime(0))
	tOID       = reflect.TypeOf(primitive.ObjectID{})
	tBinary    = reflect.TypeOf(primitive.Binary{})
	tDecimal   = reflect.TypeOf(primitive.Decimal128{})
	tTimestamp = reflect.TypeOf(primitive.Timestamp{})
)

func decimalEncodeValue(_ bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tDecimal {
		return bsoncodec.ValueEncoderError{Name: "Decimal128EncodeValue", Types: []reflect.Type{tDecimal}, Received: val}
	}

	s, isOk := val.Interface().(primitive.Decimal128)
	if !isOk {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue not objectID", Types: []reflect.Type{tOID}, Received: val}
	}

	parsedFloat, err := strconv.ParseFloat(s.String(), 64)
	if err != nil {
		return err
	}

	return vw.WriteDouble(parsedFloat)
}

func dateTimeEncodeValue(_ bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tDateTime {
		return bsoncodec.ValueEncoderError{Name: "DateTimeEncodeValue", Types: []reflect.Type{tDateTime}, Received: val}
	}

	ints, err := strconv.Atoi(fmt.Sprint(val))
	if err != nil {
		return err
	}

	t := time.Unix(0, int64(ints)*1000000).UTC()
	return vw.WriteString(t.Format(ext.ISO8601))
}

func objectIDEncodeValue(_ bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tOID {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue", Types: []reflect.Type{tOID}, Received: val}
	}

	s, isOk := val.Interface().(primitive.ObjectID)
	if !isOk {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue not objectID", Types: []reflect.Type{tOID}, Received: val}
	}

	return vw.WriteString(s.Hex())
}

func binaryEncodeValue(_ bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tBinary {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue", Types: []reflect.Type{tBinary}, Received: val}
	}

	s, isOk := val.Interface().(primitive.Binary)
	if !isOk {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue not Binary", Types: []reflect.Type{tBinary}, Received: val}
	}

	parsedUUID, err := uuid.FromBytes(s.Data)
	if err != nil {
		return err
	}

	return vw.WriteString(parsedUUID.String())
}

func timestampEncodeValue(_ bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tTimestamp {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue", Types: []reflect.Type{tBinary}, Received: val}
	}

	s, isOk := val.Interface().(primitive.Timestamp)
	if !isOk {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue not Binary", Types: []reflect.Type{tBinary}, Received: val}
	}

	return vw.WriteString(time.Unix(int64(s.T), 0).UTC().Format(ext.ISO8601))
}

func createCustomRegistry() *bsoncodec.RegistryBuilder {
	var primitiveCodecs bson.PrimitiveCodecs
	rb := bsoncodec.NewRegistryBuilder()
	bsoncodec.DefaultValueEncoders{}.RegisterDefaultEncoders(rb)
	bsoncodec.DefaultValueDecoders{}.RegisterDefaultDecoders(rb)

	rb.RegisterTypeEncoder(tDateTime, bsoncodec.ValueEncoderFunc(dateTimeEncodeValue))
	rb.RegisterTypeEncoder(tOID, bsoncodec.ValueEncoderFunc(objectIDEncodeValue))
	rb.RegisterTypeEncoder(tBinary, bsoncodec.ValueEncoderFunc(binaryEncodeValue))
	rb.RegisterTypeEncoder(tDecimal, bsoncodec.ValueEncoderFunc(decimalEncodeValue))
	rb.RegisterTypeEncoder(tTimestamp, bsoncodec.ValueEncoderFunc(timestampEncodeValue))
	primitiveCodecs.RegisterPrimitiveCodecs(rb)
	return rb
}
