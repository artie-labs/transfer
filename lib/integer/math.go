package integer

// Max will return the largest number out of the list
// If there isn't a number, it will return -1
// If there's two of the same, it will return one of them.
func Max(numbers ...int) int {
	if len(numbers) <= 0 {
		return -1
	}

	maxNumber := numbers[0]
	for _, num := range numbers[1:] {
		if num > maxNumber {
			maxNumber = num
		}
	}

	return maxNumber
}
