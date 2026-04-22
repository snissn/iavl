package cache

import (
	ibytes "github.com/cosmos/iavl/internal/bytes"
)

// Node represents a node eligible for caching.
//
// Implementations must return an immutable key for the lifetime of the cache
// entry. The cache reuses that byte slice for zero-copy lookups.
type Node interface {
	GetKey() []byte
}

// Cache is an in-memory structure to persist nodes for quick access.
// Please see lruCache for more details about why we need a custom
// cache implementation.
type Cache interface {
	// Adds node to cache. If full and had to remove the oldest element,
	// returns the oldest, otherwise nil.
	// CONTRACT: node can never be nil. Otherwise, cache panics.
	Add(node Node) Node

	// Returns Node for the key, if exists. nil otherwise.
	Get(key []byte) Node

	// Has returns true if node with key exists in cache, false otherwise.
	Has(key []byte) bool

	// Remove removes node with key from cache. The removed node is returned.
	// if not in cache, return nil.
	Remove(key []byte) Node

	// Len returns the cache length.
	Len() int
}

// lruCache is an LRU cache implementation.
// The motivation for using a custom cache implementation is to
// allow for a custom max policy.
//
// Currently, the cache maximum is implemented in terms of the
// number of nodes which is not intuitive to configure.
// Instead, we are planning to add a byte maximum.
// The alternative implementations do not allow for
// customization and the ability to estimate the byte
// size of the cache.
type lruCache struct {
	dict            map[string]*cacheEntry
	maxElementCount int
	head            *cacheEntry
	tail            *cacheEntry
	len             int
}

var _ Cache = (*lruCache)(nil)

type cacheEntry struct {
	key  string
	node Node
	prev *cacheEntry
	next *cacheEntry
}

func New(maxElementCount int) Cache {
	return &lruCache{
		dict:            make(map[string]*cacheEntry),
		maxElementCount: maxElementCount,
	}
}

func (c *lruCache) Add(node Node) Node {
	key := ibytes.UnsafeBytesToStr(node.GetKey())
	if entry, exists := c.dict[key]; exists {
		c.moveToFront(entry)
		old := entry.node
		entry.node = node
		return old
	}

	entry := &cacheEntry{key: key, node: node}
	c.pushFront(entry)
	c.dict[key] = entry
	c.len++

	if c.len > c.maxElementCount {
		return c.remove(c.tail)
	}
	return nil
}

func (c *lruCache) Get(key []byte) Node {
	if entry, hit := c.dict[ibytes.UnsafeBytesToStr(key)]; hit {
		c.moveToFront(entry)
		return entry.node
	}
	return nil
}

func (c *lruCache) Has(key []byte) bool {
	_, exists := c.dict[ibytes.UnsafeBytesToStr(key)]
	return exists
}

func (c *lruCache) Len() int {
	return c.len
}

func (c *lruCache) Remove(key []byte) Node {
	if entry, exists := c.dict[ibytes.UnsafeBytesToStr(key)]; exists {
		return c.remove(entry)
	}
	return nil
}

func (c *lruCache) moveToFront(entry *cacheEntry) {
	if entry == nil || c.head == entry {
		return
	}
	c.unlink(entry)
	c.linkFront(entry)
}

func (c *lruCache) pushFront(entry *cacheEntry) {
	if entry == nil {
		return
	}
	entry.prev = nil
	entry.next = nil
	c.linkFront(entry)
}

func (c *lruCache) linkFront(entry *cacheEntry) {
	entry.prev = nil
	entry.next = c.head
	if c.head != nil {
		c.head.prev = entry
	} else {
		c.tail = entry
	}
	c.head = entry
}

func (c *lruCache) unlink(entry *cacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}
	entry.prev = nil
	entry.next = nil
}

func (c *lruCache) remove(entry *cacheEntry) Node {
	if entry == nil {
		return nil
	}
	c.unlink(entry)
	delete(c.dict, entry.key)
	c.len--
	return entry.node
}
