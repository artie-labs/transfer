package maputil

import (
	"iter"
	"slices"
	"strings"
)

func removeFromSlice[T any](slice []T, s int) []T {
	return append(slice[:s], slice[s+1:]...)
}

type OrderedMap[T any] struct {
	keys []string
	// data - Important: Do not ever expose `data` out, always use Get, Add, Remove methods as it will cause corruption between `data` and `keys`
	data map[string]T
	// caseSensitive - if true - will preserve original casing, else it will lowercase everything
	caseSensitive bool
}

func NewOrderedMap[T any](caseSensitive bool) *OrderedMap[T] {
	return &OrderedMap[T]{
		keys:          []string{},
		data:          make(map[string]T),
		caseSensitive: caseSensitive,
	}
}

func (o *OrderedMap[T]) Remove(key string) (removed bool) {
	if !o.caseSensitive {
		key = strings.ToLower(key)
	}

	if index := slices.Index(o.keys, key); index >= 0 {
		delete(o.data, key)
		o.keys = removeFromSlice(o.keys, index)
		return true
	}

	return false
}

func (o *OrderedMap[T]) Add(key string, value T) {
	if !o.caseSensitive {
		key = strings.ToLower(key)
	}

	// Does the key already exist?
	// Only add it to `keys` if it doesn't exist
	if _, ok := o.Get(key); !ok {
		o.keys = append(o.keys, key)
	}

	o.data[key] = value
}

func (o *OrderedMap[T]) Get(key string) (T, bool) {
	if !o.caseSensitive {
		key = strings.ToLower(key)
	}

	val, ok := o.data[key]
	return val, ok
}

func (o *OrderedMap[T]) NotEmpty() bool {
	return len(o.data) > 0
}

func (o *OrderedMap[T]) Keys() []string {
	return slices.Clone(o.keys)
}

// All returns an in-order iterator over key-value pairs.
func (o *OrderedMap[T]) All() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		for _, key := range o.keys {
			if value, ok := o.Get(key); ok {
				if !yield(key, value) {
					break
				}
			}
		}
	}
}
