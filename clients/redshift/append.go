package redshift

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData) error {
	return fmt.Errorf("redshift: %s did not implement this yet", s.Label())
}
