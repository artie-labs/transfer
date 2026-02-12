package iceberg

import (
	"context"
	"strings"
	"time"
)

type IcebergCatalog interface {
	ListTables(ctx context.Context, namespace string) ([]Table, error)
	GetTableMetadata(ctx context.Context, namespace, name string) (TableMetadata, error)
	GetNamespace(ctx context.Context, name string) (string, error)
	ListNamespaces(ctx context.Context) ([]string, error)
	CreateNamespace(ctx context.Context, name string) error
	DropTable(ctx context.Context, namespace, name string) error
}

type Table struct {
	Name       string     `json:"name"`
	Namespace  string     `json:"namespace"`
	CreatedAt  *time.Time `json:"createdAt"`
	ModifiedAt *time.Time `json:"modifiedAt"`
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

func NamespaceNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "NoSuchNamespaceException: Namespace")
}
