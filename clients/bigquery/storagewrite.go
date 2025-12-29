package bigquery

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/artie-labs/transfer/clients/bigquery/converters"
	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	libconverters "github.com/artie-labs/transfer/lib/typing/converters"
)

// columnToTableFieldSchema returns a [*storagepb.TableFieldSchema] suitable for transferring data of the type that the column specifies.
// Not that the data type is not necessarily the data type that the table in the database is using.
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
	case typing.Date.Kind:
		fieldType = storagepb.TableFieldSchema_DATE
	case typing.Time.Kind:
		fieldType = storagepb.TableFieldSchema_TIME
	case typing.TimestampNTZ.Kind:
		fieldType = storagepb.TableFieldSchema_DATETIME
	case typing.TimestampTZ.Kind:
		fieldType = storagepb.TableFieldSchema_TIMESTAMP
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

	messageDescriptor, err := typing.AssertType[protoreflect.MessageDescriptor](descriptor)
	if err != nil {
		return nil, err
	}

	return &messageDescriptor, nil
}

const (
	microLength = 20
	secondShift = 0
	minuteShift = 6
	hourShift   = 12
	dayShift    = 17
	monthShift  = 22
	yearShift   = 26
)

// This is a reimplementation of https://github.com/googleapis/java-bigquerystorage/blob/f79acb5cfdd12253bca1c41551c478400120d2f9/google-cloud-bigquerystorage/src/main/java/com/google/cloud/bigquery/storage/v1/CivilTimeEncoder.java#L143
// See https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/latest/com.google.cloud.bigquery.storage.v1.CivilTimeEncoder
// And https://cloud.google.com/pubsub/docs/bigquery#date_time_int
func encodePacked64TimeMicros(value time.Time) int64 {
	return int64(encodePacked32TimeSeconds(value))<<microLength | int64(value.Nanosecond()/1000)
}

// This is a reimplementation of https://github.com/googleapis/java-bigquerystorage/blob/f79acb5cfdd12253bca1c41551c478400120d2f9/google-cloud-bigquerystorage/src/main/java/com/google/cloud/bigquery/storage/v1/CivilTimeEncoder.java#L92
func encodePacked32TimeSeconds(t time.Time) int32 {
	var bitFieldTimeSeconds int32
	bitFieldTimeSeconds |= int32(t.Hour()) << hourShift
	bitFieldTimeSeconds |= int32(t.Minute()) << minuteShift
	bitFieldTimeSeconds |= int32(t.Second()) << secondShift
	return bitFieldTimeSeconds
}

// This is a reimplementation of https://github.com/googleapis/java-bigquerystorage/blob/f79acb5cfdd12253bca1c41551c478400120d2f9/google-cloud-bigquerystorage/src/main/java/com/google/cloud/bigquery/storage/v1/CivilTimeEncoder.java#L187
func encodePacked64DatetimeSeconds(dateTime time.Time) int64 {
	var bitFieldDatetimeSeconds int64
	bitFieldDatetimeSeconds |= int64(dateTime.Year() << yearShift)
	bitFieldDatetimeSeconds |= int64(dateTime.Month() << monthShift)
	bitFieldDatetimeSeconds |= int64(dateTime.Day() << dayShift)
	bitFieldDatetimeSeconds |= int64(encodePacked32TimeSeconds(dateTime.UTC()))
	return bitFieldDatetimeSeconds
}

// This is a reimplementation of https://github.com/googleapis/java-bigquerystorage/blob/f79acb5cfdd12253bca1c41551c478400120d2f9/google-cloud-bigquerystorage/src/main/java/com/google/cloud/bigquery/storage/v1/CivilTimeEncoder.java#L248
func encodePacked64DatetimeMicros(dateTime time.Time) int64 {
	return encodePacked64DatetimeSeconds(dateTime)<<microLength | int64(dateTime.Nanosecond()/1000)
}

func rowToMessage(row map[string]any, columns []columns.Column, messageDescriptor protoreflect.MessageDescriptor, config config.Config) (*dynamicpb.Message, error) {
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
			val, err := converters.BooleanConverter{}.Convert(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value for column: %q, err: %w", column.Name(), err)
			}

			castedVal, err := typing.AssertType[bool](val)
			if err != nil {
				return nil, fmt.Errorf("failed to cast value for column: %q, err: %w", column.Name(), err)
			}

			message.Set(field, protoreflect.ValueOfBool(castedVal))
		case typing.Integer.Kind:
			val, err := converters.Int64Converter{}.Convert(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value for column: %q, err: %w", column.Name(), err)
			}

			castedValue, err := typing.AssertType[int64](val)
			if err != nil {
				return nil, fmt.Errorf("failed to cast value for column: %q, err: %w", column.Name(), err)
			}

			message.Set(field, protoreflect.ValueOfInt64(castedValue))
		case typing.Float.Kind:
			val, err := converters.Float64Converter{}.Convert(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value for column: %q, err: %w", column.Name(), err)
			}

			castedVal, err := typing.AssertType[float64](val)
			if err != nil {
				return nil, fmt.Errorf("failed to cast value for column: %q, err: %w", column.Name(), err)
			}

			message.Set(field, protoreflect.ValueOfFloat64(castedVal))
		case typing.EDecimal.Kind:
			out, err := libconverters.DecimalConverter{}.Convert(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value for column: %q, err: %w", column.Name(), err)
			}

			message.Set(field, protoreflect.ValueOfString(out))
		case typing.String.Kind:
			val, err := converters.NewStringConverter(column.KindDetails).Convert(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value for column: %q, err: %w", column.Name(), err)
			}

			castedValue, err := typing.AssertType[string](val)
			if err != nil {
				return nil, fmt.Errorf("failed to cast value for column: %q, err: %w", column.Name(), err)
			}

			message.Set(field, protoreflect.ValueOfString(castedValue))
		case typing.Date.Kind:
			_time, err := typing.ParseDateFromAny(value)
			if err != nil {
				if config.SharedDestinationSettings.SkipBadValues {
					slog.Warn("failed to cast value to date for column", slog.String("column", column.Name()), slog.Any("value", value), slog.Any("error", err))
					continue
				} else {
					return nil, fmt.Errorf("failed to cast value for column: %q, err: %w", column.Name(), err)
				}
			}

			daysSinceEpoch := _time.Unix() / (60 * 60 * 24)
			message.Set(field, protoreflect.ValueOfInt32(int32(daysSinceEpoch)))
		case typing.Time.Kind:
			_time, err := typing.ParseTimeFromAny(value)
			if err != nil {
				if config.SharedDestinationSettings.SkipBadTimestamps {
					slog.Warn("failed to cast value to time for column", slog.String("column", column.Name()), slog.Any("value", value), slog.Any("error", err))
					continue
				} else {
					return nil, fmt.Errorf("failed to cast value for column: %q, err: %w", column.Name(), err)
				}
			}

			message.Set(field, protoreflect.ValueOfInt64(encodePacked64TimeMicros(_time)))
		case typing.TimestampNTZ.Kind:
			_time, err := typing.ParseTimestampNTZFromAny(value)
			if err != nil {
				if config.SharedDestinationSettings.SkipBadTimestamps {
					slog.Warn("failed to cast value to timestampNTZ for column", slog.String("column", column.Name()), slog.Any("value", value), slog.Any("error", err))
					continue
				} else {
					return nil, fmt.Errorf("failed to cast value for column: %q, err: %w", column.Name(), err)
				}
			}

			message.Set(field, protoreflect.ValueOfInt64(encodePacked64DatetimeMicros(_time)))
		case typing.TimestampTZ.Kind:
			_time, err := typing.ParseTimestampTZFromAny(value)
			if err != nil {
				if config.SharedDestinationSettings.SkipBadTimestamps {
					slog.Warn("failed to cast value to timestampTZ for column", slog.String("column", column.Name()), slog.Any("value", value), slog.Any("error", err))
					continue
				} else {
					return nil, fmt.Errorf("failed to cast value for column: %q, err: %w", column.Name(), err)
				}
			}

			if err = timestamppb.New(_time).CheckValid(); err != nil {
				if config.SharedDestinationSettings.SkipBadTimestamps {
					slog.Warn("value is not a valid timestamp", slog.String("column", column.Name()), slog.Any("value", _time), slog.Any("error", err))
					continue
				} else {
					return nil, fmt.Errorf("column: %q, value: %q is not a valid timestamp: %w", column.Name(), _time.String(), err)
				}
			}

			message.Set(field, protoreflect.ValueOfInt64(_time.UnixMicro()))
		case typing.Struct.Kind:
			stringValue, err := encodeStructToJSONString(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value for column: %q, err: %w", column.Name(), err)
			} else if stringValue == "" {
				continue
			} else {
				message.Set(field, protoreflect.ValueOfString(stringValue))
			}
		case typing.Array.Kind:
			values, err := array.InterfaceToArrayString(value, true)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value for column: %q, err: %w", column.Name(), err)
			}
			list := message.Mutable(field).List()
			for _, val := range values {
				list.Append(protoreflect.ValueOfString(val))
			}
		default:
			return nil, fmt.Errorf("unsupported column kind: %q", column.KindDetails.Kind)
		}
	}
	return message, nil
}

// encodeStructToJSONString takes a struct as either a string or Go object and encodes it into a JSON string.
// Structs from relational and Mongo are different.
// MongoDB will return the native objects back such as `map[string]any{"hello": "world"}`
// Relational will return a string representation of the struct such as `{"hello": "world"}`
func encodeStructToJSONString(value any) (string, error) {
	if stringValue, ok := value.(string); ok {
		if stringValue == "" {
			return "", nil
		}

		if strings.Contains(stringValue, constants.ToastUnavailableValuePlaceholder) {
			return fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder), nil
		}

		// If the value is invalid JSON, then let's wrap it in quotes, so it doesn't get misinterpreted as a JSON string by BigQuery.
		// Use json.Marshal to properly escape the string for JSON format (not Go's %q format).
		if !json.Valid([]byte(stringValue)) {
			bytes, err := json.Marshal(stringValue)
			if err != nil {
				return "", fmt.Errorf("failed to marshal string value: %w", err)
			}
			return string(bytes), nil
		}

		return stringValue, nil
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("failed to marshal value: %w", err)
	}

	return string(bytes), nil
}
