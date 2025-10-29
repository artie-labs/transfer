package relational

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Debezium struct{}

func (Debezium) GetEventFromBytes(bytes []byte) (cdc.Event, error) {
	if len(bytes) == 0 {
		return nil, fmt.Errorf("empty message")
	}

	var event util.SchemaEventPayload
	if err := json.Unmarshal(bytes, &event); err != nil {
		return nil, err
	}

	return &event, nil
}

func (Debezium) Labels() []string {
	return []string{
		constants.DBZPostgresFormat,
		constants.DBZPostgresAltFormat,
		constants.DBZMySQLFormat,
		constants.DBZRelationalFormat,
	}
}

func (Debezium) GetPrimaryKey(key []byte, tc kafkalib.TopicConfig, reservedColumns []string) (map[string]any, error) {
	return debezium.ParsePartitionKey(key, tc.CDCKeyFormat, reservedColumns)
}
