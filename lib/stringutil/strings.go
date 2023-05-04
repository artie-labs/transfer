package stringutil

import (
	"fmt"
	"strings"
)

func Reverse(val string) string {
	var reverseParts []rune
	valRune := []rune(val)
	for i := len(val) - 1; i >= 0; i-- {
		reverseParts = append(reverseParts, valRune[i])
	}

	return string(reverseParts)
}

func Wrap(colVal interface{}) string {
	// Escape line breaks, JSON_PARSE does not like it.
	colVal = strings.ReplaceAll(fmt.Sprint(colVal), `\`, `\\`)
	// The normal string escape is to do for O'Reilly is O\\'Reilly, but Snowflake escapes via \'
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
