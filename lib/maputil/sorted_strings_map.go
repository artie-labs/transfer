package maputil

import (
	"iter"
	"slices"
	"sort"
)

type SortedStringsMap[T any] struct {
	keys []string
	data map[string]T
}

func NewSortedStringsMap[T any]() *SortedStringsMap[T] {
	return &SortedStringsMap[T]{
		keys: []string{},
		data: make(map[string]T),
	}
}

func (s *SortedStringsMap[T]) Add(key string, value T) {
	if _, ok := s.data[key]; !ok {
		s.keys = append(s.keys, key)
		// This can be more efficient in the future once we get more usage.
		sort.Strings(s.keys)
	}

	s.data[key] = value
}

func (s *SortedStringsMap[T]) Get(key string) (T, bool) {
	val, ok := s.data[key]
	return val, ok
}

func (s *SortedStringsMap[T]) Keys() []string {
	return slices.Clone(s.keys)
}

func (s *SortedStringsMap[T]) All() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		for _, key := range s.keys {
			if value, ok := s.Get(key); ok {
				if !yield(key, value) {
					break
				}
			}
		}
	}
}
