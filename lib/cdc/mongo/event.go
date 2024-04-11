package mongo

import (
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

	Source    Source `json:"source"`
	Operation string `json:"op"`

	// These maps are used to store the before and after JSONE as a map, since `before` and `after` come in as a JSONE string.
	beforeMap map[string]any
	afterMap  map[string]any
}

func (p *Payload) SetAfterMap(afterMap map[string]any) {
	p.afterMap = afterMap
}

func (p *Payload) GetAfterMap() map[string]any {
	return p.afterMap
}

func (p *Payload) SetBeforeMap(beforeMap map[string]any) {
	p.beforeMap = beforeMap
}

func (p *Payload) GetBeforeMap() map[string]any {
	return p.beforeMap
}

type Source struct {
	Connector  string `json:"connector"`
	TsMs       int64  `json:"ts_ms"`
	Database   string `json:"db"`
	Collection string `json:"collection"`
}
