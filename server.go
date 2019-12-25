package main

import (
	"bufio"
	"context"
	"log"
	"net"
	"strings"
)

type Server struct {
	listenAddress string
	whynot        *Whynot
}

func NewServer(listenAddress string, nodesList []string) (*Server, error) {
	whynot, err := NewWhynot(nodesList)
	if err != nil {
		return nil, err
	}
	return &Server{
		listenAddress: listenAddress,
		whynot:        whynot,
	}, nil
}

func (server *Server) Run(ctx context.Context) error {
	socket, err := net.Listen("tcp", server.listenAddress)
	if err != nil {
		return err
	}
	defer func() {
		if err := socket.Close(); err != nil {
			log.Println(err)
		}
	}()

	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			conn, err := socket.Accept()
			if err != nil {
				errc <- err
				return
			}
			go server.accept(conn, errc)
		}
	}()

	log.Println("started listen", server.listenAddress)
	select {
	case <-ctx.Done():
		return nil
	case err := <-errc:
		return err
	}
}

func (server *Server) accept(conn net.Conn, errc chan error) {
	closeListen := func() {
		if err := conn.Close(); err != nil {
			errc <- err
			return
		}
	}
	defer closeListen()

	var response string
	defer func() {
		if _, err := conn.Write([]byte(response + "\n")); err != nil {
			errc <- err
			return
		}
	}()

	message, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		response = err.Error()
		errc <- err
		return
	}
	cmd := strings.TrimSpace(string(message))
	log.Print("<-", cmd)
	if response, err = server.whynot.Process(cmd); err != nil {
		response = err.Error()
	}
}
