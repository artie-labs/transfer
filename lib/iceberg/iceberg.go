package iceberg

import (
	"context"
	"time"
)

type IcebergCatalog interface {
	Ping(ctx context.Context) error
	ListTables(ctx context.Context, namespace string) ([]Table, error)
	GetTableMetadata(ctx context.Context, namespace, name string) (TableMetadata, error)
	ListNamespaces(ctx context.Context) ([]string, error)
	CreateNamespace(ctx context.Context, name string) error
	DropTable(ctx context.Context, namespace, name string) error
	SweepTempTables(ctx context.Context, schemas []string) error
}

type Table struct {
	Name      string     `json:"name"`
	Namespace string     `json:"namespace"`
	CreatedAt *time.Time `json:"createdAt"`
}

type TableMetadata struct {
	TableARN        *string    `json:"tableARN"`
	CreatedAt       *time.Time `json:"createdAt"`
	ModifiedAt      *time.Time `json:"modifiedAt"`
	CurrentSchemaID int        `json:"currentSchemaID"`
	Location        string     `json:"location"`
	Columns         []Column   `json:"columns"`
}

type Column struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}
