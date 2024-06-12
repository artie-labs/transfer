package bigquery

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// From https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/latest/com.google.cloud.bigquery.storage.v1.CivilTimeEncoder
// And https://cloud.google.com/pubsub/docs/bigquery#date_time_int
func encodePacked64TimeMicros(value time.Time) int64 {
	var result = int64(value.Nanosecond() / 1000)
	result |= int64(value.Second()) << 20
	result |= int64(value.Minute()) << 26
	result |= int64(value.Hour()) << 32
	return result
}

func rowToMessage(row map[string]any, columns []columns.Column, messageDescriptor protoreflect.MessageDescriptor, additionalDateFmts []string) (*dynamicpb.Message, error) {
	message := dynamicpb.NewMessage(messageDescriptor)

	for _, column := range columns {
		field := message.Descriptor().Fields().ByTextName(column.Name())
		if field == nil {
			return nil, fmt.Errorf("failed to find a field named %q", column.Name())
		}

		value := row[column.Name()]

		fmt.Printf("%s %v %s %T %v\n", column.Name(), column.KindDetails, field.Kind(), value, value)

		if value == nil {
			// message.Set(field, protoreflect.Value{})
			continue
		}

		switch column.KindDetails.Kind {
		case typing.Boolean.Kind:
			if boolValue, ok := value.(bool); ok {
				message.Set(field, protoreflect.ValueOfBool(boolValue))
			} else {
				return nil, fmt.Errorf("expected bool received %T with value %v", value, value)
			}
		case typing.Integer.Kind:
			switch value := value.(type) {
			case int32:
				message.Set(field, protoreflect.ValueOfInt32(value))
			case int64:
				message.Set(field, protoreflect.ValueOfInt64(value))
			case int:
				message.Set(field, protoreflect.ValueOfInt64(int64(value)))
			default:
				return nil, fmt.Errorf("expected int/int32/int64 received %T with value %v", value, value)
			}
		case typing.Float.Kind:
			switch value := value.(type) {
			case float32:
				message.Set(field, protoreflect.ValueOfFloat32(value))
			case float64:
				message.Set(field, protoreflect.ValueOfFloat64(value))
			default:
				return nil, fmt.Errorf("expected float32/float64 recieved %T with value %v", value, value)
			}
		case typing.String.Kind:
			if stringValue, ok := value.(string); ok {
				message.Set(field, protoreflect.ValueOfString(stringValue))
			} else {
				return nil, fmt.Errorf("expected string received %T with value %v", value, value)
			}
		case typing.EDecimal.Kind:
			if decimalValue, ok := value.(*decimal.Decimal); ok {
				message.Set(field, protoreflect.ValueOf(decimalValue.Value()))
			} else {
				return nil, fmt.Errorf("expected *decimal.Decimal received %T with value %v", decimalValue, decimalValue)
			}
		case typing.ETime.Kind:
			extTime, err := ext.ParseFromInterface(value, additionalDateFmts)
			if err != nil {
				return nil, fmt.Errorf("failed to cast value as time.Time, value: %v, err: %w", value, err)
			}

			if column.KindDetails.ExtendedTimeDetails == nil {
				return nil, fmt.Errorf("column kind details for extended time details is null")
			}

			switch column.KindDetails.ExtendedTimeDetails.Type {
			case ext.DateKindType:
				daysSinceEpoch := extTime.Unix() / (60 * 60 * 24)
				message.Set(field, protoreflect.ValueOfInt64(daysSinceEpoch))
			case ext.TimeKindType:
				message.Set(field, protoreflect.ValueOfInt64(encodePacked64TimeMicros(extTime.Time)))
			case ext.DateTimeKindType:
				ts := timestamppb.New(extTime.Time)
				if err := ts.CheckValid(); err != nil {
					return nil, err
				}
				message.Set(field, protoreflect.ValueOfInt64(extTime.UnixMicro()))
			default:
				return nil, fmt.Errorf("error")
			}
		case typing.Struct.Kind:
			if stringValue, ok := value.(string); ok {
				message.Set(field, protoreflect.ValueOfString(stringValue))
			} else {
				return nil, fmt.Errorf("expected string received %T with value %v", value, value)
			}

			// bytes, err := json.Marshal(value)
			// if err != nil {
			// 	return nil, err
			// }

			// msg := message.Mutable(field).Message().(protoreflect.ProtoMessage)
			// err = protojson.Unmarshal(bytes, msg)
			// if err != nil {
			// 	return nil, fmt.Errorf("fail: %w", err)
			// }

			// message.Set(field, protoreflect.ValueOfString(string(bytes)))
		case typing.Array.Kind:
			value, err := array.InterfaceToArrayString(value, true)
			if err != nil {
				return nil, err
			}
			list := message.Mutable(field).List()
			for _, j := range value {
				list.Append(protoreflect.ValueOf(j))
			}
		default:
			return nil, fmt.Errorf("error")
		}

	}
	return message, nil
}
