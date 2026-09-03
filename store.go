package main

import "net"

type Store interface {
	Set(key []byte, val []byte)
	Get(key []byte, val []byte) ([]byte, bool)
	Delete(key []byte)
	List(conn net.Conn, buffer *[]byte)
}
