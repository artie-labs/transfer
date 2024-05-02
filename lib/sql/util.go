package sql

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/stringutil"
)

// QuoteLiteral wraps a string with single quotes so that it can be used in a SQL query.
// If there are backslashes in the string, then they will be escaped to [\\].
// After escaping backslashes, any remaining single quotes will be replaced with [\'].
func QuoteLiteral(value string) string {
	return fmt.Sprintf("'%s'", strings.ReplaceAll(stringutil.EscapeBackslashes(value), "'", `\'`))
}

func QuoteIdentifiers(identifiers []string, dialect Dialect) []string {
	result := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		result[i] = dialect.QuoteIdentifier(identifier)
	}
	return result
}
