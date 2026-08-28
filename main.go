package main

import (
	"errors"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Server struct {
	listener     net.Listener // listener interface
	listenerAddr string       // port
	storage      *contactBookMap
	isClosed     bool

	mu    sync.Mutex
	connM map[net.Conn]struct{}
}

func NewServer(listenerAddr string, storage *contactBookMap) *Server {
	return &Server{
		listenerAddr: listenerAddr,
		storage:      storage,
		connM:        make(map[net.Conn]struct{}),
	}
}

func (s *Server) Start() error {
	var err error

	osSignalChan := make(chan os.Signal, 1)
	waitGroup := sync.WaitGroup{}

	s.listener, err = net.Listen("tcp", s.listenerAddr)
	if err != nil {
		return err
	}

	signal.Notify(osSignalChan, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(osSignalChan)

	go func() {
		<-osSignalChan
		log.Println("Server Shutdown")
		s.Close()
	}()

	for {
		conn, err := s.listener.Accept() // performs the three-way handshake under the hood
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) { // client disconnect or server shutdown
				break
			}
			log.Printf("Connection Error %v\n", err)
			continue
		}
		s.mu.Lock()
		if s.isClosed { // reads valeu
			s.mu.Unlock()
			conn.Close()
			continue
		}
		waitGroup.Add(1)
		s.connM[conn] = struct{}{}
		s.mu.Unlock()
		go process(s, conn, s.storage, &waitGroup)
	}

	waitGroup.Wait()
	return nil
}

func (s *Server) Close() {

	s.listener.Close()

	s.mu.Lock()
	s.isClosed = true
	cons := make([]net.Conn, 0, len(s.connM)) // reading the map
	for k := range s.connM {                  // only keys
		cons = append(cons, k)
	}
	s.mu.Unlock()

	for _, conn := range cons {
		conn.Close() // to unblock and wake up conn.Read()
	}
}

func main() {
	contactBook := NewContactBookMap()
	serverAddr := NewServer(":3000", contactBook)
	if err := serverAddr.Start(); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
	log.Println("Server gracefully shutdown")
}
