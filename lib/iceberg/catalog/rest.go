package catalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	icebergTypes "github.com/artie-labs/transfer/lib/iceberg"
)

type Config struct {
	URI        string
	Token      string
	Credential string
	Warehouse  string
	Prefix     string
}

type RestCatalog struct {
	catalog *rest.Catalog
}

func NewRESTCatalog(ctx context.Context, cfg Config) (*RestCatalog, error) {
	var opts []rest.Option
	if cfg.Token != "" {
		opts = append(opts, rest.WithOAuthToken(cfg.Token))
	}
	if cfg.Credential != "" {
		opts = append(opts, rest.WithCredential(cfg.Credential))
	}
	if cfg.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(cfg.Warehouse))
	}
	if cfg.Prefix != "" {
		opts = append(opts, rest.WithPrefix(cfg.Prefix))
	}

	cat, err := rest.NewCatalog(ctx, "iceberg", cfg.URI, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST catalog: %w", err)
	}

	return &RestCatalog{catalog: cat}, nil
}

// ListTables returns all tables in the given namespace.
func (r *RestCatalog) ListTables(ctx context.Context, namespace string) ([]icebergTypes.Table, error) {
	var tables []icebergTypes.Table
	for ident, err := range r.catalog.ListTables(ctx, buildNamespaceToIdentifier(namespace)) {
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}
		tables = append(tables, icebergTypes.Table{
			Name:      catalog.TableNameFromIdent(ident),
			Namespace: namespace,
			CreatedAt: nil, // REST catalog doesn't provide creation time in list
		})
	}
	return tables, nil
}

// GetTableMetadata loads table metadata for the given namespace and table name.
func (r *RestCatalog) GetTableMetadata(ctx context.Context, namespace, name string) (icebergTypes.TableMetadata, error) {
	tbl, err := r.catalog.LoadTable(ctx, buildNamespaceToIdentifier(namespace, name))
	if err != nil {
		return icebergTypes.TableMetadata{}, fmt.Errorf("failed to load table: %w", err)
	}

	metadata := tbl.Metadata()
	schema := metadata.CurrentSchema()

	var columns []icebergTypes.Column
	for _, field := range schema.Fields() {
		columns = append(columns, icebergTypes.Column{
			ID:       field.ID,
			Name:     field.Name,
			Type:     icebergTypeToString(field.Type),
			Required: field.Required,
		})
	}

	return icebergTypes.TableMetadata{
		TableARN:        nil, // REST catalog doesn't have ARN
		CreatedAt:       nil, // Would need to parse from properties
		ModifiedAt:      nil, // Would need to parse from properties
		CurrentSchemaID: schema.ID,
		Location:        metadata.Location(),
		Columns:         columns,
	}, nil
}

// GetNamespace checks if a namespace exists and returns its name.
func (r *RestCatalog) GetNamespace(ctx context.Context, name string) (string, error) {
	if _, err := r.catalog.LoadNamespaceProperties(ctx, buildNamespaceToIdentifier(name)); err != nil {
		return "", fmt.Errorf("failed to load namespace properties: %w", err)
	}

	return name, nil
}

// ListNamespaces returns all namespaces in the catalog.
func (r *RestCatalog) ListNamespaces(ctx context.Context) ([]string, error) {
	namespaces, err := r.catalog.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}

	var result []string
	for _, ns := range namespaces {
		// Namespace is a slice of strings representing the hierarchy
		if len(ns) > 0 {
			result = append(result, strings.Join(ns, "."))
		}
	}
	return result, nil
}

// CreateNamespace creates a new namespace in the catalog.
func (r *RestCatalog) CreateNamespace(ctx context.Context, name string) error {
	if err := r.catalog.CreateNamespace(ctx, buildNamespaceToIdentifier(name), nil); err != nil {
		return fmt.Errorf("failed to create namespace: %w", err)
	}
	return nil
}

// DropTable removes a table from the catalog.
func (r *RestCatalog) DropTable(ctx context.Context, namespace, name string) error {
	if err := r.catalog.DropTable(ctx, buildNamespaceToIdentifier(namespace, name)); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	return nil
}

// buildNamespaceToIdentifier converts namespace string(s) to a table.Identifier.
// If multiple parts are provided (e.g., namespace and table name), they are combined.
// Namespace parts separated by "." are split into individual components.
func buildNamespaceToIdentifier(parts ...string) table.Identifier {
	var result table.Identifier
	for _, part := range parts {
		// Split by "." to handle hierarchical namespaces like "db.schema"
		result = append(result, strings.Split(part, ".")...)
	}
	return result
}

func icebergTypeToString(t iceberg.Type) string {
	if t == nil {
		return "unknown"
	}
	return t.String()
}
