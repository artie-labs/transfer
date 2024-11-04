package batch

import (
	"fmt"
	"log/slog"
)

type KeyFunction interface {
	Key() string
}

func hasKeyFunction[T any](item T) (KeyFunction, bool) {
	if castedItem, isOk := any(item).(KeyFunction); isOk {
		return castedItem, isOk
	}

	return nil, false
}

// BySize takes a series of elements [in], encodes them using [encode], groups them into batches of bytes that sum to at
// most [maxSizeBytes], and then passes each batch to the [yield] function.
func BySize[T any](in []T, maxSizeBytes int, failIfExceedMaxSizeBytes bool, encode func(T) ([]byte, error), yield func([][]byte) error) error {
	var buffer [][]byte
	var currentSizeBytes int

	for i, item := range in {
		bytes, err := encode(item)
		if err != nil {
			return fmt.Errorf("failed to encode item %d: %w", i, err)
		}

		if len(bytes) > maxSizeBytes {
			if failIfExceedMaxSizeBytes {
				return fmt.Errorf("item %d is larger (%d bytes) than maxSizeBytes (%d bytes)", i, len(bytes), maxSizeBytes)
			} else {
				logFields := []any{slog.Int("index", i), slog.Int("bytes", len(bytes))}
				if stringItem, isOk := hasKeyFunction[T](item); isOk {
					logFields = append(logFields, slog.String("key", stringItem.Key()))
				}

				slog.Warn("Skipping item as the row is larger than maxSizeBytes", logFields...)
				continue
			}
		}

		currentSizeBytes += len(bytes)
		if currentSizeBytes < maxSizeBytes {
			buffer = append(buffer, bytes)
		} else if currentSizeBytes == maxSizeBytes {
			buffer = append(buffer, bytes)
			if err = yield(buffer); err != nil {
				return err
			}
			buffer = [][]byte{}
			currentSizeBytes = 0
		} else {
			if err = yield(buffer); err != nil {
				return err
			}
			buffer = [][]byte{bytes}
			currentSizeBytes = len(bytes)
		}
	}

	if len(buffer) > 0 {
		if err := yield(buffer); err != nil {
			return err
		}
	}

	return nil
}
