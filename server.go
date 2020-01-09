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

type Request struct {
	Message string
	Address string
}

func makeRequest(input, address string) (*Request, error) {
	message := strings.TrimSpace(input)
	return &Request{
		Message: message,
		Address: address,
	}, nil
}

type Response struct {
	Message string
}

func newResponse(message string) *Response {
	return &Response{Message: message}
}

func (server *Server) accept(conn net.Conn, errc chan error) {
	closeListen := func() {
		if err := conn.Close(); err != nil {
			errc <- err
			return
		}
	}
	defer closeListen()

	input, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		if _, err := conn.Write([]byte(err.Error() + "\n")); err != nil {
			errc <- err
			return
		}
		errc <- err
		return
	}
	message, err := makeRequest(input, conn.RemoteAddr().String())
	if err != nil {
		if _, err := conn.Write([]byte(err.Error() + "\n")); err != nil {
			errc <- err
			return
		}
		return
	}
	log.Print("this <-", message.Address, message.Message)
	responses := make(chan *Response)
	go func() {
		defer close(responses)
		if err = server.whynot.Process(*message, responses); err != nil {
			if _, err := conn.Write([]byte(err.Error() + "\n")); err != nil {
				errc <- err
				return
			}
			return
		}
	}()
	for result := range responses {
		log.Println("this ->", message.Address, result)
		if _, err := conn.Write([]byte(result.Message + "\n")); err != nil {
			errc <- err
			return
		}
	}
}
