package main

import (
	"log"
	"net"
	"strconv"
	"sync"
)

type shardedContactBookMap struct { // big database with a count of
	table      []*shard // slice of shards of dataset
	shardCount int      // number of shards N ?
}

type shard struct {
	mu sync.RWMutex
	m  map[string]Record
}

func fnv1a(data []byte) uint32 { // gives us the random hash
	const (
		base  uint32 = 2166136261
		prime uint32 = 16777619
	)
	hash := base

	for _, b := range data {
		hash ^= uint32(b)
		hash *= prime

	}
	return uint32(hash)
}

func (s *shardedContactBookMap) getShardIndex(key []byte) uint32 {
	if len(s.table) == 1 {
		return 0
	}
	hash := fnv1a(key)
	nShards := uint32(len(s.table))
	return (nShards - 1) & hash
}

func computeShardCount(shardCount int) int {
	if shardCount <= 16 {
		return 16
	}

	n := shardCount - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16

	return n + 1
}

func MakeConcurrentMap(shardCount int, preAllocatedSpace int) *shardedContactBookMap {
	if shardCount == 1 {
		table := []*shard{{m: make(map[string]Record, preAllocatedSpace)}}
		return &shardedContactBookMap{table: table, shardCount: 1}
	}
	shardCount = computeShardCount(shardCount)
	preAllocatedSpace = preAllocatedSpace / shardCount

	if preAllocatedSpace < shardCount {
		preAllocatedSpace = 1
	}

	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{m: make(map[string]Record, preAllocatedSpace)}
	}
	return &shardedContactBookMap{table: table, shardCount: shardCount}
}

type contactBookMap struct {
	contactBook map[string]Record
	lock        sync.RWMutex
	HWM         int
	counterOPS  int
}

type Record struct {
	data []byte
	ID   int
}

func (c *contactBookMap) Set(key []byte, val []byte) { //recieve a struct which has a pointer
	c.lock.Lock()
	defer c.lock.Unlock()

	c.counterOPS++ // 1 - Key : Value

	copyVal := make([]byte, len(val)) // this is the 24byte struct, allocated on the function execution stack frame; this contains a pointer that points to the backing array that is on the heap
	copy(copyVal, val)                //dst, src
	c.contactBook[string(key)] = Record{
		data: copyVal,
		ID:   c.counterOPS,
	} // increased the length of the map

	if len(c.contactBook) > c.HWM {
		c.HWM = len(c.contactBook)
	}
}

func (c *contactBookMap) Get(key []byte, copyVal []byte) ([]byte, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	rec, ok := c.contactBook[string(key)]
	if !ok {
		return nil, false
	}

	n := copy(copyVal, rec.data)
	return copyVal[:n], true // only returns the n # of bytes that was copied and not the entire 1024 slice
}

func (c *contactBookMap) Delete(key []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()

	currentHWM := c.HWM
	delete(c.contactBook, string(key))
	if len(c.contactBook) < (currentHWM/4) && len(c.contactBook) > 8 {
		copyMap := make(map[string]Record, len(c.contactBook))
		for k, v := range c.contactBook {
			copyMap[k] = v
		}
		c.contactBook = copyMap
		c.HWM = len(c.contactBook)
	}
}

type ghost struct {
	ID   int
	key  string
	data []byte
}

func (c *contactBookMap) List(conn net.Conn, buffer *[]byte) {
	c.lock.RLock()

	tempMap := make([]ghost, len(c.contactBook))
	i := 0
	for k, v := range c.contactBook {
		tempMap[i].ID = v.ID
		tempMap[i].key = k
		tempMap[i].data = v.data
		i++
	}

	c.lock.RUnlock()

	window := (*buffer) // has a different nature than conn.Read(...) about the cap/len
	window = window[:0]
	for j := range tempMap { // ID_-_k_:_v\n
		digitLength := digitLength(tempMap[j].ID)
		currentLength := len(window) + digitLength + 7 + len(tempMap[j].data) + len(tempMap[j].key)
		if currentLength > 1024 {
			_, err := conn.Write(window)
			if err != nil {
				log.Printf("Error has occured: %v \n", err)
				return
			}
			window = window[:0]
		}
		window = strconv.AppendInt(window, int64(tempMap[j].ID), 10)
		window = append(window, " - "...)
		window = append(window, tempMap[j].key...)
		window = append(window, " : "...)
		window = append(window, tempMap[j].data...)
		window = append(window, "\n"...)
	}
	if len(window) != 0 {
		_, err := conn.Write(window)
		if err != nil {
			log.Printf("Error has occured: %v \n", err)
			return
		}
	}
}

func digitLength(input int) int {
	if input == 0 {
		return 1
	}

	length := 0

	for input != 0 {
		length++
		input = input / 10
	}
	return length
}

func NewContactBookMap(n int) *contactBookMap {
	return &contactBookMap{
		contactBook: make(map[string]Record, n),
		HWM:         0,
		counterOPS:  0,
	}
}
