package bigquery

import (
	"fmt"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func columnToTableFieldSchema(column columns.Column) (*storagepb.TableFieldSchema, error) {
	var fieldType storagepb.TableFieldSchema_Type
	mode := storagepb.TableFieldSchema_NULLABLE

	switch column.KindDetails.Kind {
	case typing.Boolean.Kind:
		fieldType = storagepb.TableFieldSchema_BOOL
	case typing.Integer.Kind:
		fieldType = storagepb.TableFieldSchema_INT64
	case typing.Float.Kind:
		fieldType = storagepb.TableFieldSchema_DOUBLE
	case typing.EDecimal.Kind:
		fieldType = storagepb.TableFieldSchema_STRING
	case typing.String.Kind:
		fieldType = storagepb.TableFieldSchema_STRING
	case typing.ETime.Kind:
		switch column.KindDetails.ExtendedTimeDetails.Type {
		case ext.TimeKindType:
			fieldType = storagepb.TableFieldSchema_TIME
		case ext.DateKindType:
			fieldType = storagepb.TableFieldSchema_DATE
		case ext.DateTimeKindType:
			fieldType = storagepb.TableFieldSchema_TIMESTAMP
		default:
			return nil, fmt.Errorf("unsupported extended time details type: %q", column.KindDetails.ExtendedTimeDetails.Type)
		}
	case typing.Struct.Kind:
		fieldType = storagepb.TableFieldSchema_STRING
	case typing.Array.Kind:
		fieldType = storagepb.TableFieldSchema_STRING
		mode = storagepb.TableFieldSchema_REPEATED
	default:
		return nil, fmt.Errorf("unsupported column kind: %q", column.KindDetails.Kind)
	}

	return &storagepb.TableFieldSchema{
		Name: column.Name(),
		Type: fieldType,
		Mode: mode,
	}, nil
}

func columnsToMessageDescriptor(cols []columns.Column) (*protoreflect.MessageDescriptor, error) {
	fields := make([]*storagepb.TableFieldSchema, len(cols))
	for i, col := range cols {
		field, err := columnToTableFieldSchema(col)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}
	tableSchema := storagepb.TableSchema{Fields: fields}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(&tableSchema, "root")
	if err != nil {
		return nil, fmt.Errorf("failed to build proto descriptor: %w", err)
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("proto descriptor is not a message descriptor")
	}
	return &messageDescriptor, nil
}

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

		if value == nil {
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
			case int:
				message.Set(field, protoreflect.ValueOfInt64(int64(value)))
			case int32:
				message.Set(field, protoreflect.ValueOfInt64(int64(value)))
			case int64:
				message.Set(field, protoreflect.ValueOfInt64(value))
			default:
				return nil, fmt.Errorf("expected int/int32/int64 received %T with value %v", value, value)
			}
		case typing.Float.Kind:
			switch value := value.(type) {
			case float32:
				message.Set(field, protoreflect.ValueOfFloat64(float64(value)))
			case float64:
				message.Set(field, protoreflect.ValueOfFloat64(value))
			default:
				return nil, fmt.Errorf("expected float32/float64 recieved %T with value %v", value, value)
			}
		case typing.EDecimal.Kind:
			if decimalValue, ok := value.(*decimal.Decimal); ok {
				message.Set(field, protoreflect.ValueOfString(decimalValue.String()))
			} else {
				return nil, fmt.Errorf("expected *decimal.Decimal received %T with value %v", decimalValue, decimalValue)
			}
		case typing.String.Kind:
			var stringValue string
			switch value := value.(type) {
			case string:
				stringValue = value
			case *decimal.Decimal:
				stringValue = value.String()
			default:
				return nil, fmt.Errorf("expected string/decimal.Decimal received %T with value %v", value, value)
			}
			message.Set(field, protoreflect.ValueOfString(stringValue))
		case typing.ETime.Kind:
			extTime, err := ext.ParseFromInterface(value, additionalDateFmts)
			if err != nil {
				return nil, fmt.Errorf("failed to cast value as time.Time, value: %v, err: %w", value, err)
			}

			if column.KindDetails.ExtendedTimeDetails == nil {
				return nil, fmt.Errorf("extended time details for column kind details is nil")
			}

			switch column.KindDetails.ExtendedTimeDetails.Type {
			case ext.TimeKindType:
				message.Set(field, protoreflect.ValueOfInt64(encodePacked64TimeMicros(extTime.Time)))
			case ext.DateKindType:
				daysSinceEpoch := extTime.Unix() / (60 * 60 * 24)
				message.Set(field, protoreflect.ValueOfInt32(int32(daysSinceEpoch)))
			case ext.DateTimeKindType:
				if err := timestamppb.New(extTime.Time).CheckValid(); err != nil {
					return nil, err
				}
				message.Set(field, protoreflect.ValueOfInt64(extTime.UnixMicro()))
			default:
				return nil, fmt.Errorf("unsupported extended time details: %q", column.KindDetails.ExtendedTimeDetails.Type)
			}
		case typing.Struct.Kind:
			stringValue, err := EncodeStructToJSONString(value)
			if err != nil {
				return nil, err
			} else if stringValue == "" {
				continue
			} else {
				message.Set(field, protoreflect.ValueOfString(stringValue))
			}
		case typing.Array.Kind:
			values, err := array.InterfaceToArrayString(value, true)
			if err != nil {
				return nil, err
			}
			list := message.Mutable(field).List()
			for _, value := range values {
				list.Append(protoreflect.ValueOf(value))
			}
		default:
			return nil, fmt.Errorf("unsupported column kind: %q", column.KindDetails.Kind)
		}
	}
	return message, nil
}
