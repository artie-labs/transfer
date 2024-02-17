package bigquery

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(tableData *optimization.TableData) error {
	return fmt.Errorf("bigquery: did not implement this yet")
}
