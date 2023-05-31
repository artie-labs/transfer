package stringutil

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

func Reverse(val string) string {
	var reverseParts []rune
	valRune := []rune(val)
	for i := len(val) - 1; i >= 0; i-- {
		reverseParts = append(reverseParts, valRune[i])
	}

	return string(reverseParts)
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

func LineBreaksToCarriageReturns(paragraph string) string {
	return strings.ReplaceAll(paragraph, "\n", `\n`)
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
