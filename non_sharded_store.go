package main

import (
	"log"
	"net"
	"strconv"
	"sync"
)

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

type ns_ghost struct {
	ID   int
	key  string
	data []byte
}

func (c *contactBookMap) List(conn net.Conn, buffer *[]byte) {
	ghost_table := make([]ns_ghost, 0, 1024)
	window := (*buffer) // has a different nature than conn.Read(...) about the cap/len

	c.lock.RLock()

	for k, v := range c.contactBook {
		ghost_table = ghost_table[:0]
		ghost_table = append(ghost_table, ns_ghost{ID: v.ID, key: k, data: v.data})
	}

	c.lock.RUnlock()

	window = window[:0]

	for j := range ghost_table { // ID_-_k_:_v\n
		digitLength := digitLength(ghost_table[j].ID)
		key := ghost_table[j].key
		data := ghost_table[j].data
		ID := ghost_table[j].ID
		currentLength := len(window) + digitLength + 7 + len(data) + len(key)

		if currentLength > 1024 {
			_, err := conn.Write(window)
			if err != nil {
				log.Printf("Error has occured: %v \n", err)
				return
			}
			window = window[:0]
		}
		window = strconv.AppendInt(window, int64(ID), 10)
		window = append(window, " - "...)
		window = append(window, []byte(key)...)
		window = append(window, " : "...)
		window = append(window, data...)
		window = append(window, "\n"...)
	}

	if len(window) != 0 {
		_, err := conn.Write(window)
		if err != nil {
			log.Printf("Error has occured: %v \n", err)
			window = window[:0]
			return
		}
		window = window[:0]
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
