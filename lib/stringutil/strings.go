package stringutil

func Reverse(val string) string {
	var reverseParts []rune
	valRune := []rune(val)
	for i := len(val) - 1; i >= 0; i-- {
		reverseParts = append(reverseParts, valRune[i])
	}

	return string(reverseParts)
}
