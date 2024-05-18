package stringutil

import (
	"math/rand"
	"strings"
)

func CapitalizeFirstLetter(s string) string {
	if len(s) == 0 {
		return s
	}

	return strings.ToUpper(s[:1]) + s[1:]
}

func EscapeBackslashes(value string) string {
	return strings.ReplaceAll(value, `\`, `\\`)
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
	return stringWithCharset(length, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
}
