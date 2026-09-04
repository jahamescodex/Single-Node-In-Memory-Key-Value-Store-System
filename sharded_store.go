package main

import (
	"log"
	"net"
	"sync"
)

type shardedContactBookMap struct { // big database with a count of
	table      []*shard // slice of shards of dataset
	shardCount int      // number of shards N ?
}

type shard struct {
	mu      sync.RWMutex
	m       map[string]Record
	kvCount int
	HWM     int
}

func (c *shardedContactBookMap) Set(key []byte, val []byte) {
	idx := c.getShardIndex(key)
	shard := c.table[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.kvCount++
	copyVal := make([]byte, len(val))
	copy(copyVal, val)
	shard.m[string(key)] = Record{
		data: copyVal,
	}

	if len(shard.m) > shard.HWM {
		shard.HWM = len(shard.m)
	}
}

func (c *shardedContactBookMap) Get(key []byte, copyVal []byte) ([]byte, bool) {
	idx := c.getShardIndex(key)
	shard := c.table[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	rec, ok := shard.m[string(key)]
	if !ok {
		return nil, false
	}

	n := copy(copyVal, rec.data)
	return copyVal[:n], true // only returns the n # of bytes that was copied and not the entire 1024 slice
}

func (c *shardedContactBookMap) Delete(key []byte) {
	idx := c.getShardIndex(key)
	shard := c.table[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	currentHWM := shard.HWM
	delete(shard.m, string(key))
	if len(shard.m) < (currentHWM/4) && len(shard.m) > 8 {
		copyMap := make(map[string]Record, len(shard.m))
		for k, v := range shard.m {
			copyMap[k] = v
		}
		shard.m = copyMap
		shard.HWM = len(shard.m)
	}
}

type s_ghost struct {
	key  string
	data []byte
}

func (c *shardedContactBookMap) List(conn net.Conn, buffer *[]byte) { // table of pointer to shards (mini maps)
	ghost_table := make([]s_ghost, 0, 1024)
	window := (*buffer)

	for idx := range c.table { //table of shards
		ghost_table = ghost_table[:0] // reset len
		shard := c.table[idx]

		shard.mu.RLock()
		for k, v := range shard.m {
			ghost_table = append(ghost_table, s_ghost{key: k, data: v.data})
		}
		shard.mu.RUnlock()

		window = window[:0]

		for j := range ghost_table {

			key := ghost_table[j].key
			data := ghost_table[j].data

			currentLength := len(window) + len(key) + len(data) + 4
			if currentLength > 1024 {
				_, err := conn.Write(window)
				if err != nil {
					log.Printf("Error has occured: %v \n", err)
					return
				}
				window = window[:0]
			}

			window = append(window, key...)
			window = append(window, " : "...)
			window = append(window, data...)
			window = append(window, "\n"...)
		}

		if len(window) != 0 {
			_, err := conn.Write(window)
			if err != nil {
				log.Printf("Error has occured: %v \n", err)
				return
			}
			window = window[:0]
		}
	}
}
func fnv1a(key []byte) uint32 { // gives us the random hash
	const (
		prime uint32 = 0x01000193
		base  uint32 = 0x811C9DC5
	)
	hash := base

	for _, b := range []byte(key) {
		hash ^= uint32(b)
		hash *= prime

	}
	return uint32(hash)
}

func (s *shardedContactBookMap) getShardIndex(key []byte) uint32 {
	if s.shardCount == 1 {
		return 0
	}
	hash := fnv1a(key)
	nShards := uint32(s.shardCount)
	return (nShards - 1) & hash
}

func computeShardCount(shardCount int) int {
	if shardCount <= 0 {
		return 1
	}

	u := uint32(shardCount) // 2^31
	u--
	u |= u >> 1
	u |= u >> 2
	u |= u >> 4
	u |= u >> 8
	u |= u >> 16
	u++

	if u > 1<<30 || u == 0 {
		return 1 << 30
	}

	return int(u)
}

func MakeShardedMap(shardCount int, preAllocatedSpace int) *shardedContactBookMap {
	shardCount = computeShardCount(shardCount)
	if preAllocatedSpace < 0 {
		preAllocatedSpace = 0
	}

	perShardSpace := preAllocatedSpace / shardCount
	if perShardSpace <= 0 {
		perShardSpace = 1
	}

	table := make([]*shard, shardCount)

	for i := 0; i < shardCount; i++ {
		table[i] = &shard{m: make(map[string]Record, perShardSpace)}
	}
	return &shardedContactBookMap{
		table:      table,
		shardCount: shardCount,
	}
}
