package stringutil

import (
	"math/rand"
	"strings"
	"unicode/utf8"
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

// ReplaceInvalidUTF8 re-encodes invalid UTF-8 bytes by treating each one as a Latin-1 code point.
// Latin-1 (ISO 8859-1) maps directly to Unicode U+0000..U+00FF, so this is lossless for Latin-1 source data.
func ReplaceInvalidUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}

	var buf strings.Builder
	buf.Grow(len(s) + len(s)/4)
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 {
			buf.WriteRune(rune(s[i]))
			i++
		} else {
			buf.WriteRune(r)
			i += size
		}
	}
	return buf.String()
}
