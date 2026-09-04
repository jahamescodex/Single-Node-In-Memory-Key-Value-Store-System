package main

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"sync"
)

var empty = []byte("Command Line cannot be empty\n")
var invalid = []byte("Invalid: Does not have enough arguments\n")
var size = []byte("ERROR: Arguments too big\n")
var emptyVal = []byte("Null\n")
var success = []byte("Success\n")
var nonCommand = []byte("Please enter a valid command\n")

var bufferPool = sync.Pool{
	New: func() any {
		bufHeader := make([]byte, 1024)
		return &bufHeader
	},
}

func process(s *Server, conn net.Conn, c Store, parentWaitGroup *sync.WaitGroup) {
	defer parentWaitGroup.Done()

	defer func() {
		s.mu.Lock()
		delete(s.connM, conn)
		s.mu.Unlock()
	}()

	defer conn.Close() // LIFO

	handleClient(conn, c, &bufferPool)
}

func handleClient(conn net.Conn, c Store, bufferPool *sync.Pool) {
	log.Printf("Client: %s just connected\n", conn.RemoteAddr())
	buffHeaderPtr := bufferPool.Get().(*[]byte) // pointing to the 24-byte struct byte slice-header

	defer func() {
		log.Printf("Client: %s just disconnected, buffer put back into pool", conn.RemoteAddr())
		clear(*buffHeaderPtr) // dereferences to gain access to the underlying back array that points to the actual information
		// and clears it with its associated zero value
		bufferPool.Put(buffHeaderPtr) // returning the pointer of the 24-byte struct back into the pool
	}()

	processed := 0

	for {
		fullBuffer := (*buffHeaderPtr)[:cap(*buffHeaderPtr)] //move out of for loop maybe?

		n, err := conn.Read(fullBuffer[processed:]) // research about connection reset by peer : never sending a clean FIN handshake
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return
			}
			log.Println("Error:", err) // socket has transitioned out of the ESTABLISHED state
			return
		}
		processed += n
		for {
			idx := bytes.IndexByte(fullBuffer[:processed], '\n')
			if idx == -1 && processed == cap(fullBuffer) { // invalid input, overflow; conservative: malicious clients
				conn.Write(size)
				return
			}
			if idx == -1 { // broken command missing \n
				break
			} else {
				commandLine := fullBuffer[:idx+1] // cuts the 'ribbon' into the command line
				execute(conn, commandLine, c, bufferPool)
				copy(fullBuffer, fullBuffer[idx+1:processed]) // shifts the ribbon back
				processed -= (idx + 1)
			}
		}
	}
}

func execute(conn net.Conn, commandLine []byte, c Store, bufferPool *sync.Pool) {
	commandLine = bytes.TrimSpace(commandLine) // clears leading /n /r or white spaces

	if len(commandLine) == 0 {
		conn.Write(empty)
		return
	}

	split := bytes.SplitN(commandLine, []byte(" "), 3) // slice of byte slices [ []byte, []byte ]
	command := split[0]                                // ['Get'] in binary
	args := split[1:]

	for i := range command {
		if command[i] >= 'a' && command[i] <= 'z' {
			command[i] -= 32
		}
	}

	switch string(command) {
	case "SET":
		if len(args) != 2 {
			conn.Write(invalid)
			return
		}
		c.Set((args[0]), args[1])
		conn.Write(success)
	case "GET":
		if len(args) != 1 {
			conn.Write(invalid)
			return
		}
		buffHeaderPtr := bufferPool.Get().(*[]byte)
		dst := *buffHeaderPtr
		defer func() {
			clear(*buffHeaderPtr)
			bufferPool.Put(buffHeaderPtr)
		}()
		dst = dst[:1024] //stretches length up to cap
		dst, ok := c.Get((args[0]), dst)
		if !ok {
			conn.Write(emptyVal)
			return
		}
		conn.Write(dst)
		conn.Write(success)
	case "DELETE":
		if len(args) != 1 {
			conn.Write(invalid)
			return
		}
		c.Delete(args[0])
		conn.Write(success)
	case "LIST":
		buffer := bufferPool.Get().(*[]byte)
		defer func() {
			clear(*buffer)
			bufferPool.Put(buffer)
		}()
		c.List(conn, buffer)
	default:
		conn.Write(nonCommand)
	}
}
