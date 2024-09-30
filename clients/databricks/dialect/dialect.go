package dialect

import (
	"fmt"
)

type DatabricksDialect struct{}

func (DatabricksDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func (DatabricksDialect) EscapeStruct(value string) string {
	panic("not implemented")
}
