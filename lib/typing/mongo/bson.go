package mongo

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"time"
)

var (
	tDateTime = reflect.TypeOf(primitive.DateTime(0))
	tOID      = reflect.TypeOf(primitive.ObjectID{})
)

func dateTimeEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {

	const jDateFormat = "2006-01-02T15:04:05.999Z"
	if !val.IsValid() || val.Type() != tDateTime {
		return bsoncodec.ValueEncoderError{Name: "DateTimeEncodeValue", Types: []reflect.Type{tDateTime}, Received: val}
	}

	ints := val.Int()
	t := time.Unix(0, ints*1000000).UTC()

	return vw.WriteString(t.Format(jDateFormat))
}

func objectIDEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {

	if !val.IsValid() || val.Type() != tOID {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue", Types: []reflect.Type{tOID}, Received: val}
	}
	s := val.Interface().(primitive.ObjectID).Hex()
	return vw.WriteString(s)
}

func createCustomRegistry() *bsoncodec.RegistryBuilder {
	var primitiveCodecs bson.PrimitiveCodecs
	rb := bsoncodec.NewRegistryBuilder()
	bsoncodec.DefaultValueEncoders{}.RegisterDefaultEncoders(rb)
	bsoncodec.DefaultValueDecoders{}.RegisterDefaultDecoders(rb)
	rb.RegisterEncoder(tDateTime, bsoncodec.ValueEncoderFunc(dateTimeEncodeValue))
	rb.RegisterEncoder(tOID, bsoncodec.ValueEncoderFunc(objectIDEncodeValue))
	primitiveCodecs.RegisterPrimitiveCodecs(rb)
	return rb
}
