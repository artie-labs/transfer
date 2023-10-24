package numbers

// BetweenEq - Looks something like this. start <= number <= end
func BetweenEq(start, end, number int) bool {
	return number >= start && number <= end
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}

	return b
}
