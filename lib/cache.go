package lib

import "sync"

type KVCache[T any] struct {
	mu    sync.RWMutex
	cache map[string]T
}

func NewKVCache[T any]() *KVCache[T] {
	return &KVCache[T]{
		cache: make(map[string]T),
	}
}

func (c *KVCache[T]) Get(key string) (T, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	value, ok := c.cache[key]
	return value, ok
}

func (c *KVCache[T]) Set(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[key] = value
}
