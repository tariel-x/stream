package server

import (
	"bufio"
	"context"
	"log"
	"net"
	"strings"
	"time"

	"github.com/tariel-x/whynot/stream"
)

type Server struct {
	listenAddress string
	handler       *stream.Handler
}

func NewServer(listenAddress string, handler *stream.Handler) (*Server, error) {
	return &Server{
		listenAddress: listenAddress,
		handler:       handler,
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
			go server.accept(ctx, conn, errc)
			time.Sleep(time.Second * 2)
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
	message string
	address string
}

func (r *Request) Message() string {
	return r.message
}

func (r *Request) Address() string {
	return r.address
}

func makeRequest(input, address string) (*Request, error) {
	message := strings.TrimSpace(input)
	return &Request{
		message: message,
		address: address,
	}, nil
}

type Response struct {
	messages chan string
}

func NewResponse() *Response {
	return &Response{messages: make(chan string)}
}

func (r *Response) Push(message string) {
	r.messages <- message
}

func (server *Server) accept(parent context.Context, conn net.Conn, errc chan error) {
	ctx, _ := context.WithCancel(parent)

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
	request, err := makeRequest(input, conn.RemoteAddr().String())
	if err != nil {
		if _, err := conn.Write([]byte(err.Error() + "\n")); err != nil {
			errc <- err
			return
		}
		return
	}
	log.Printf("this <- %s %s\n", request.Address(), request.Message())
	response := NewResponse()
	go func() {
		defer close(response.messages)
		if err = server.handler.Process(ctx, request, response); err != nil {
			if _, err := conn.Write([]byte(err.Error() + "\n")); err != nil {
				errc <- err
				return
			}
			return
		}
	}()
	for message := range response.messages {
		log.Printf("this -> %s %s", request.Address(), message)
		if _, err := conn.Write([]byte(message + "\n")); err != nil {
			errc <- err
			return
		}
	}
}
