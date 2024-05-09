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

// ParseDataTypeDefinition parses a column type definition returning the type and arguments.
// "TEXT" -> "TEXT", {}
// "VARCHAR(1234)" -> "VARCHAR", {"1234"}
// "NUERMIC(5, 1)" -> "NUMERIC", {"5", "1"}
func ParseDataTypeDefinition(value string) (string, []string, error) {
	value = strings.TrimSpace(value)

	if idx := strings.Index(value, "("); idx > 0 {
		if value[len(value)-1] != ')' {
			return "", nil, fmt.Errorf("missing closing parenthesis")
		}

		args := strings.Split(value[idx+1:len(value)-1], ",")
		for i, arg := range args {
			args[i] = strings.TrimSpace(arg)
		}
		return strings.TrimSpace(value[:idx]), args, nil
	}
	return value, nil, nil
}
