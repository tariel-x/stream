package server

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net"
	"strings"

	"github.com/tariel-x/stream/client"
	"github.com/tariel-x/stream/stream"
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
			//time.Sleep(time.Second)
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
	name    string
}

func (r *Request) Message() string {
	return r.message
}

func (r *Request) Address() string {
	return r.address
}

func (r *Request) Name() string {
	if r.name != "" {
		return r.name
	}
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
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	closeListen := func() {
		if err := conn.Close(); err != nil {
			errc <- err
			return
		}
	}
	defer closeListen()

	rawinput, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		if _, err := conn.Write([]byte(err.Error() + "\n")); err != nil {
			errc <- err
			return
		}
		errc <- err
		return
	}

	input, meta, err := server.extractMeta(rawinput)
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
			log.Printf("error parsing query from %s: %s", conn.RemoteAddr().String(), err)
			return
		}
		return
	}
	if name, ok := meta[client.MetaKeyName]; ok {
		request.name = name
	}

	log.Printf("this <- %s %s\n", request.Name(), request.Message())
	response := NewResponse()
	go func() {
		defer close(response.messages)
		if err = server.handler.Process(ctx, request, response); err != nil {
			if _, err := conn.Write([]byte(err.Error() + "\n")); err != nil {
				log.Println("error executing query", err)
				return
			}
			return
		}
	}()
	for message := range response.messages {
		log.Printf("this -> %s %s", request.Name(), message)
		if _, err := conn.Write([]byte(message + "\n")); err != nil {
			log.Println("error writing to client", err)
			return
		}
	}
}

func (server *Server) extractMeta(rawinput string) (string, map[string]string, error) {
	inputparts := strings.Split(rawinput, ";")
	input := inputparts[0]
	meta := map[string]string{}
	for i := 1; i < len(inputparts); i++ {
		metaparts := strings.Split(inputparts[i], "=")
		if len(metaparts) != 2 {
			return "", nil, errors.New("invalid meta")
		}
		meta[metaparts[0]] = metaparts[1]
	}
	return input, meta, nil
}
