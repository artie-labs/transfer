package mongo

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
)

// SchemaEventPayload is our struct for an event with schema enabled. For reference, this is an example payload https://gist.github.com/Tang8330/d0998d8d1ebcbeaa4ecb8e098445cc3a
type SchemaEventPayload struct {
	Schema  debezium.Schema `json:"schema"`
	Payload Payload         `json:"payload"`
}

type Payload struct {
	Before *string `json:"before"`
	After  *string `json:"after"`

	Source    Source              `json:"source"`
	Operation constants.Operation `json:"op"`

	// These maps are used to store the before and after JSONE as a map, since `before` and `after` come in as a JSONE string.
	beforeMap map[string]any
	afterMap  map[string]any
}

type Source struct {
	Connector  string `json:"connector"`
	TsMs       int64  `json:"ts_ms"`
	Database   string `json:"db"`
	Collection string `json:"collection"`
}
