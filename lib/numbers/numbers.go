package numbers

// BetweenEq - Looks something like this. start <= number <= end
func BetweenEq[T int | int32 | int64](start, end, number T) bool {
	return number >= start && number <= end
}
