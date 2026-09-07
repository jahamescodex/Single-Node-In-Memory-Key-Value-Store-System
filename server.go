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
var newLine = []byte("\n")

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

	handleClient(conn, c)
}

func handleClient(conn net.Conn, c Store) {
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
				handleCommand(conn, commandLine, c)
				copy(fullBuffer, fullBuffer[idx+1:processed]) // shifts the ribbon back
				processed -= (idx + 1)
			}
		}
	}
}

func handleCommand(conn net.Conn, commandLine []byte, c Store) {
	commandLine = bytes.TrimSpace(commandLine) // clears leading /n /r or white spaces

	if len(commandLine) == 0 {
		conn.Write(empty)
		return
	}

	cmd, arg, success := bytes.Cut(commandLine, []byte(" "))

	for i := 0; i < len(cmd); i++ {
		if cmd[i] >= 'a' && cmd[i] <= 'z' {
			cmd[i] -= 32
		}
	}

	if !success {
		execute(cmd, nil, nil, c, conn)
		return
	}

	key, value, success := bytes.Cut(arg, []byte(" "))
	if !success {
		execute(cmd, arg, nil, c, conn)
		return
	}

	execute(cmd, key, value, c, conn)
}

func execute(command []byte, key []byte, value []byte, c Store, conn net.Conn) {
	switch string(command) {
	case "SET":
		if key == nil || value == nil {
			conn.Write(invalid)
			return
		}
		c.Set(key, value)
		conn.Write(success)
	case "GET":
		if key == nil {
			conn.Write(invalid)
			return
		}
		buffHeaderPtr := bufferPool.Get().(*[]byte)
		dst := *buffHeaderPtr
		defer func() {
			*buffHeaderPtr = (*buffHeaderPtr)[:cap((*buffHeaderPtr))]
			clear(*buffHeaderPtr)
			bufferPool.Put(buffHeaderPtr)
		}()
		dst, exists := c.Get(key, dst)
		if !exists {
			conn.Write(emptyVal)
			return
		}
		conn.Write(dst)
		conn.Write(newLine)
	case "DELETE":
		if key == nil {
			conn.Write(invalid)
			return
		}
		c.Delete(key)
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
