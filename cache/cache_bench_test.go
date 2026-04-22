package cache_test

import (
	"container/list"
	"math/rand"
	"testing"

	"github.com/cosmos/iavl/cache"
	ibytes "github.com/cosmos/iavl/internal/bytes"
)

func BenchmarkAdd(b *testing.B) {
	b.ReportAllocs()
	testcases := map[string]struct {
		cacheMax int
		keySize  int
	}{
		"small - max: 10K, key size - 10b": {
			cacheMax: 10000,
			keySize:  10,
		},
		"med - max: 100K, key size 20b": {
			cacheMax: 100000,
			keySize:  20,
		},
		"large - max: 1M, key size 30b": {
			cacheMax: 1000000,
			keySize:  30,
		},
	}

	for name, tc := range testcases {
		cache := cache.New(tc.cacheMax)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				key := randBytes(tc.keySize)
				b.StartTimer()

				_ = cache.Add(&testNode{
					key: key,
				})
			}
		})
	}
}

func BenchmarkRemove(b *testing.B) {
	b.ReportAllocs()

	cache := cache.New(1000)
	existentKeyMirror := [][]byte{}
	// Populate cache
	for i := 0; i < 50; i++ {
		key := randBytes(1000)

		existentKeyMirror = append(existentKeyMirror, key)

		cache.Add(&testNode{
			key: key,
		})
	}

	randSeed := 498727689 // For deterministic tests
	r := rand.New(rand.NewSource(int64(randSeed)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := existentKeyMirror[r.Intn(len(existentKeyMirror))]
		_ = cache.Remove(key)
	}
}

func BenchmarkAddFixedNodeKeys(b *testing.B) {
	b.ReportAllocs()
	testcases := map[string]struct {
		cacheMax int
		keySize  int
	}{
		"node-key-sized": {
			cacheMax: 16384,
			keySize:  12,
		},
		"medium-key": {
			cacheMax: 16384,
			keySize:  32,
		},
	}

	for name, tc := range testcases {
		c := cache.New(tc.cacheMax)
		keys := make([][]byte, 4096)
		for i := range keys {
			keys[i] = randBytes(tc.keySize)
		}
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				key := keys[i%len(keys)]
				_ = c.Add(&testNode{key: key})
			}
		})
	}
}

func BenchmarkGetFixedNodeKeys(b *testing.B) {
	b.ReportAllocs()
	c := cache.New(16384)
	keys := make([][]byte, 4096)
	for i := range keys {
		keys[i] = randBytes(12)
		c.Add(&testNode{key: keys[i]})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Get(keys[i%len(keys)])
	}
}

func BenchmarkCacheImplementations_NodeKeys(b *testing.B) {
	keys := make([][]byte, 4096)
	for i := range keys {
		keys[i] = randBytes(12)
	}

	b.Run("add/current", func(b *testing.B) {
		c := cache.New(16384)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			key := makeUniqueNodeKey(i)
			node := &testNode{key: key}
			b.StartTimer()
			_ = c.Add(node)
		}
	})

	b.Run("add/legacy", func(b *testing.B) {
		c := newLegacyCache(16384)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			key := makeUniqueNodeKey(i)
			node := &testNode{key: key}
			b.StartTimer()
			_ = c.Add(node)
		}
	})

	b.Run("get/current", func(b *testing.B) {
		c := cache.New(16384)
		for _, key := range keys {
			c.Add(&testNode{key: key})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = c.Get(keys[i%len(keys)])
		}
	})

	b.Run("get/legacy", func(b *testing.B) {
		c := newLegacyCache(16384)
		for _, key := range keys {
			c.Add(&testNode{key: key})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = c.Get(keys[i%len(keys)])
		}
	})
}

type legacyCache struct {
	dict            map[string]*list.Element
	maxElementCount int
	ll              *list.List
}

func newLegacyCache(maxElementCount int) *legacyCache {
	return &legacyCache{
		dict:            make(map[string]*list.Element),
		maxElementCount: maxElementCount,
		ll:              list.New(),
	}
}

func (c *legacyCache) Add(node cache.Node) cache.Node {
	key := node.GetKey()
	if e, exists := c.dict[string(key)]; exists {
		c.ll.MoveToFront(e)
		old := e.Value
		e.Value = node
		return old.(cache.Node)
	}

	elem := c.ll.PushFront(node)
	c.dict[string(key)] = elem

	if c.ll.Len() > c.maxElementCount {
		oldest := c.ll.Back()
		return c.remove(oldest)
	}
	return nil
}

func (c *legacyCache) Get(key []byte) cache.Node {
	if elem, hit := c.dict[string(key)]; hit {
		c.ll.MoveToFront(elem)
		return elem.Value.(cache.Node)
	}
	return nil
}

func (c *legacyCache) remove(e *list.Element) cache.Node {
	removed := c.ll.Remove(e).(cache.Node)
	delete(c.dict, ibytes.UnsafeBytesToStr(removed.GetKey()))
	return removed
}

func makeUniqueNodeKey(i int) []byte {
	key := make([]byte, 12)
	key[4] = byte(i >> 56)
	key[5] = byte(i >> 48)
	key[6] = byte(i >> 40)
	key[7] = byte(i >> 32)
	key[8] = byte(i >> 24)
	key[9] = byte(i >> 16)
	key[10] = byte(i >> 8)
	key[11] = byte(i)
	return key
}
