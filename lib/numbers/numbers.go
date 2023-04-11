package numbers

// BetweenEq - Looks something like this. start <= number <= end
func BetweenEq(start, end, number int) bool {
	return number >= start && number <= end
}
