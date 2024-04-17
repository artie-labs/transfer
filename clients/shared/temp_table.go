package shared

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/optimization"
)

func TempTableName(dwh destination.DataWarehouse, tableData *optimization.TableData) string {
	return fmt.Sprintf("%s_%s", dwh.ToFullyQualifiedName(tableData.TableIdentifier(), false), tableData.TempTableSuffix())
}
