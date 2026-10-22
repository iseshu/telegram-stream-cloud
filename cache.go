package main

import (
	"container/list"
	"sync"
)

const chunkSize int64 = 512 * 1024

type cacheEntry struct {
	key  string
	data []byte
}
type BlockCache struct {
	mu                      sync.Mutex
	cap, used, hits, misses int64
	items                   map[string]*list.Element
	lru                     *list.List
}

func newCache(capacity int64) *BlockCache {
	return &BlockCache{cap: capacity, items: map[string]*list.Element{}, lru: list.New()}
}
func (c *BlockCache) Get(k string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[k]
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	c.lru.MoveToFront(e)
	return e.Value.(cacheEntry).data, true
}

// Entries are immutable; callers must never modify a returned buffer.
func (c *BlockCache) Put(k string, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if int64(len(b)) > c.cap {
		return
	}
	if _, ok := c.items[k]; ok {
		return
	}
	for c.used+int64(len(b)) > c.cap {
		e := c.lru.Back()
		v := e.Value.(cacheEntry)
		delete(c.items, v.key)
		c.used -= int64(len(v.data))
		c.lru.Remove(e)
	}
	data := append([]byte(nil), b...)
	c.items[k] = c.lru.PushFront(cacheEntry{k, data})
	c.used += int64(len(data))
}
func (c *BlockCache) Stats() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]int64{"bytes": c.used, "limit": c.cap, "entries": int64(len(c.items)), "hits": c.hits, "misses": c.misses}
}
