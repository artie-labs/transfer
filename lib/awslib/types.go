package awslib

import (
	"fmt"

	"github.com/google/uuid"
)

// Ref: https://iceberg.apache.org/spec/#table-metadata-and-snapshots
type S3TableSchema struct {
	FormatVersion      int                 `json:"format-version"`
	TableUUID          uuid.UUID           `json:"table-uuid"`
	Location           string              `json:"location"`
	LastSequenceNumber int                 `json:"last-sequence-number"`
	LastUpdatedMS      int                 `json:"last-updated-ms"`
	CurrentSchemaID    int                 `json:"current-schema-id"`
	Schemas            []InnerSchemaObject `json:"schemas"`
}

func (s S3TableSchema) RetrieveCurrentSchema() (InnerSchemaObject, error) {
	for _, schema := range s.Schemas {
		if schema.SchemaID == s.CurrentSchemaID {
			return schema, nil
		}
	}

	return InnerSchemaObject{}, fmt.Errorf("current schema not found")
}

type InnerSchemaObject struct {
	Type     string             `json:"struct"`
	SchemaID int                `json:"schema-id"`
	Fields   []InnerSchemaField `json:"fields"`
}

type InnerSchemaField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}
