package stringutil

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Override - pass in a list of vals, the right most value that is not empty will override.
func Override(vals ...string) string {
	var retVal string
	for _, val := range vals {
		if val != "" {
			retVal = val
		}
	}

	return retVal
}

func Wrap(colVal interface{}, noQuotes bool) string {
	colVal = strings.ReplaceAll(fmt.Sprint(colVal), `\`, `\\`)
	// The normal string escape is to do for O'Reilly is O\\'Reilly, but Snowflake escapes via \'
	if noQuotes {
		return fmt.Sprint(colVal)
	}

	// When there is quote wrapping `foo -> 'foo'`, we'll need to escape `'` so the value compiles.
	// However, if there are no quote wrapping, we should not need to escape.
	return fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprint(colVal), "'", `\'`))
}

func Empty(vals ...string) bool {
	for _, val := range vals {
		if val == "" {
			return true
		}
	}

	return false
}

func EscapeSpaces(col string) (escaped bool, newString string) {
	subStr := " "
	return strings.Contains(col, subStr), strings.ReplaceAll(col, subStr, "__")
}

func stringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func Random(length int) string {
	rand.Seed(time.Now().UnixNano())
	return stringWithCharset(length, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
}
