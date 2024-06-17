package batch

import "fmt"

// BySize takes a series of elements, encodes them, groups them into batches of bytes that sum to at most [maxSize], and
// then passes each of those batches to the [yield] function.
func BySize[T any](in []T, maxSize int, encode func(T) ([]byte, error), yield func([][]byte) error) error {
	var buffer [][]byte
	var currentSize int

	for i, item := range in {
		bytes, err := encode(item)
		if err != nil {
			return fmt.Errorf("failed to encode item %d: %w", i, err)
		}

		if len(bytes) > maxSize {
			return fmt.Errorf("item %d is larger (%d bytes) than max size (%d bytes)", i, len(bytes), maxSize)
		}

		currentSize += len(bytes)

		if currentSize < maxSize {
			buffer = append(buffer, bytes)
		} else if currentSize == maxSize {
			buffer = append(buffer, bytes)
			if err := yield(buffer); err != nil {
				return err
			}
			buffer = [][]byte{}
			currentSize = 0
		} else {
			if err := yield(buffer); err != nil {
				return err
			}
			buffer = [][]byte{bytes}
			currentSize = len(bytes)
		}
	}

	if len(buffer) > 0 {
		if err := yield(buffer); err != nil {
			return err
		}
	}

	return nil
}
