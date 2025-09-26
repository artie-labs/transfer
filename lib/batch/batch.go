package batch

import (
	"fmt"
	"log/slog"
)

type KeyFunction interface {
	Key() string
}

func hasKeyFunction[T any](item T) (KeyFunction, bool) {
	if castedItem, ok := any(item).(KeyFunction); ok {
		return castedItem, ok
	}

	return nil, false
}

// BySize takes a series of elements [in], encodes them using [encode], groups them into batches of bytes that sum to at
// most [maxSizeBytes], and then passes each batch to the [yield] function.
func BySize[T any](in []T, maxSizeBytes int, failIfRowExceedsMaxSizeBytes bool, encode func(T) ([]byte, error), yield func([][]byte, []T) error) (int, error) {
	var buffer [][]byte
	var rows []T
	var currentSizeBytes int
	var skipped int

	for i, item := range in {
		bytes, err := encode(item)
		if err != nil {
			return 0, fmt.Errorf("failed to encode item %d: %w", i, err)
		}

		if len(bytes) > maxSizeBytes {
			if failIfRowExceedsMaxSizeBytes {
				return 0, fmt.Errorf("item %d is larger (%d bytes) than maxSizeBytes (%d bytes)", i, len(bytes), maxSizeBytes)
			} else {
				logFields := []any{slog.Int("index", i), slog.Int("bytes", len(bytes))}
				if stringItem, ok := hasKeyFunction[T](item); ok {
					logFields = append(logFields, slog.String("key", stringItem.Key()))
				}

				slog.Warn("Skipping item as the row is larger than maxSizeBytes", logFields...)
				skipped++
				continue
			}
		}

		currentSizeBytes += len(bytes)
		if currentSizeBytes < maxSizeBytes {
			buffer = append(buffer, bytes)
			rows = append(rows, item)
		} else if currentSizeBytes == maxSizeBytes {
			buffer = append(buffer, bytes)
			rows = append(rows, item)
			if err = yield(buffer, rows); err != nil {
				return 0, err
			}
			buffer = [][]byte{}
			rows = []T{}
			currentSizeBytes = 0
		} else {
			if err = yield(buffer, rows); err != nil {
				return 0, err
			}
			buffer = [][]byte{bytes}
			rows = []T{item}
			currentSizeBytes = len(bytes)
		}
	}

	if len(buffer) > 0 {
		if err := yield(buffer, rows); err != nil {
			return 0, err
		}
	}

	return skipped, nil
}
