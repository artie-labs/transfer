package sql

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/stringutil"
)

func QuoteLiteral(value string) string {
	// When there is quote wrapping `foo -> 'foo'`, we'll need to escape `'` so the value compiles.
	// However, if there are no quote wrapping, we should not need to escape.
	return fmt.Sprintf("'%s'", strings.ReplaceAll(stringutil.EscapeBackslashes(value), "'", `\'`))
}
