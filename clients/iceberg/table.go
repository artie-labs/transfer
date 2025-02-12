package iceberg

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s Store) GetTableConfig(tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	// TODO:
	return nil, fmt.Errorf("not implemented")
}

func (s Store) CreateTable(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column) error {
	// TODO:
	return fmt.Errorf("not implemented")
}

func (s Store) AlterTableAddColumns(ctx context.Context, tableConfig *types.DestinationTableConfig, tableID sql.TableIdentifier, cols []columns.Column) error {
	if len(cols) == 0 {
		// Nothing to add, early return.
		return nil
	}

	// TODO:
	return fmt.Errorf("not implemented")
}
