package mongo

import "github.com/artie-labs/transfer/lib/debezium"

// SchemaEventPayload is our struct for an event with schema enabled. For reference, this is an example payload https://gist.github.com/Tang8330/d0998d8d1ebcbeaa4ecb8e098445cc3a
type SchemaEventPayload struct {
	Schema  debezium.Schema `json:"schema"`
	Payload payload         `json:"payload"`
}

type payload struct {
	Before    *string `json:"before"`
	After     *string `json:"after"`
	BeforeMap map[string]interface{}
	AfterMap  map[string]interface{}
	Source    Source `json:"source"`
	Operation string `json:"op"`
}

type Source struct {
	Connector  string `json:"connector"`
	TsMs       int64  `json:"ts_ms"`
	Database   string `json:"db"`
	Collection string `json:"collection"`
}
