package postgres

import "github.com/artie-labs/transfer/lib/debezium"

// SchemaEventPayload is our struct for an event with schema enabled. For reference, this is an example payload https://gist.github.com/Tang8330/3b9989ed8c659771958fe481f248397a
type SchemaEventPayload struct {
	Schema  debezium.Schema `json:"schema"`
	Payload payload         `json:"payload"`
}

type payload struct {
	Before    map[string]interface{} `json:"before"`
	After     map[string]interface{} `json:"after"`
	Source    Source                 `json:"source"`
	Operation string                 `json:"op"`
}

type Source struct {
	Connector string `json:"connector"`
	TsMs      int64  `json:"ts_ms"`
	Database  string `json:"db"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
}
