package client

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	CmdPush     = "PUSH"
	CmdPull     = "PULL"
	CmdStatus   = "STATUS"
	CmdPrepare  = "PREPARE"
	CmdPromise  = "PROMISE"
	CmdRefuse   = "REFUSE"
	CmdAccept   = "ACCEPT"
	CmdAccepted = "ACCEPTED"
	CmdSet      = "SET"
	CmdOK       = "OK"
)

var (
	ErrInvalidResponse = errors.New("invalid response")
)

type Logger interface {
	Println(message ...interface{})
}

type NullLogger struct{}

func (nl *NullLogger) Println(message ...interface{}) {}

type Client struct {
	Address string
	Timeout time.Duration
	Logger  Logger
}

func New(address string, timeout *time.Duration) (*Client, error) {
	client := &Client{
		Address: address,
		Timeout: time.Second * 20,
		Logger:  &NullLogger{},
	}
	if timeout != nil {
		client.Timeout = *timeout
	}
	return client, nil
}

type Connection struct {
	Client     *Client
	connection net.Conn
}

func (c *Client) Connect() (*Connection, error) {
	conn, err := net.DialTimeout("tcp", c.Address, c.Timeout)
	if err != nil {
		return nil, err
	}
	return &Connection{
		Client:     c,
		connection: conn,
	}, nil
}

func (c *Connection) Close() error {
	return c.connection.Close()
}

type Request interface {
	String() string
}

func (c *Connection) Exec(r Request) error {
	message := r.String()
	c.Client.Logger.Println("this -> ", c.Client.Address, message)
	_, err := fmt.Fprint(c.connection, message+"\n")
	return err
}

func (c *Connection) QueryOne(r Request) (*Response, error) {
	message := r.String()
	c.Client.Logger.Println("this -> ", c.Client.Address, message)
	if _, err := fmt.Fprint(c.connection, message+"\n"); err != nil {
		return nil, err
	}
	nodeResponse, err := bufio.NewReader(c.connection).ReadString('\n')
	if err != nil {
		return nil, err
	}
	c.Client.Logger.Println("this <- ", c.Client.Address, nodeResponse)
	return &Response{Message: nodeResponse}, nil
}

type Responses struct {
	responses chan *Response
	errors    chan error
}

func (r *Responses) Next() *Response {
	if r.responses == nil {
		return nil
	}
	if response, ok := <-r.responses; ok {
		return response
	} else {
		return nil
	}
}

func (r *Responses) Err() error {
	select {
	case err := <-r.errors:
		return err
	default:
		return nil
	}
}

func (c *Connection) QueryMany(r Request) (*Responses, error) {
	message := r.String()
	c.Client.Logger.Println("this -> ", c.Client.Address, message)
	if _, err := fmt.Fprint(c.connection, message+"\n"); err != nil {
		return nil, err
	}
	responses := &Responses{
		responses: make(chan *Response),
		errors:    make(chan error),
	}
	go func() {
		defer close(responses.responses)
		defer close(responses.errors)
		for {
			nodeResponse, err := bufio.NewReader(c.connection).ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				responses.errors <- err
				continue
			}
			responses.responses <- &Response{Message: nodeResponse}
			c.Client.Logger.Println("this <- ", c.Client.Address, nodeResponse)
		}
	}()
	return responses, nil
}

func (c *Client) Exec(r Request) error {
	connection, err := c.Connect()
	if err != nil {
		return err
	}
	defer connection.Close()
	return connection.Exec(r)
}

func (c *Client) QueryOne(r Request) (*Response, error) {
	connection, err := c.Connect()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return connection.QueryOne(r)
}

func (c *Client) QueryMain(r Request) (*Responses, error) {
	connection, err := c.Connect()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return connection.QueryMany(r)
}

type Response struct {
	Message string
}

func (r *Response) Cmd() (string, string) {
	parsed := strings.SplitN(r.Message, " ", 2)
	if len(parsed) == 0 {
		return "", ""
	}
	cmd := strings.TrimSpace(parsed[0])
	if len(parsed) == 1 {
		return cmd, ""
	}
	args := strings.TrimSpace(parsed[1])
	return cmd, args
}

type Push struct {
	V string
}

func (p *Push) String() string {
	return fmt.Sprintf("%s %s", CmdPush, p.V)
}

func (r *Response) Ok() (bool, error) {
	cmd, _ := r.Cmd()
	if cmd != CmdOK && cmd != CmdRefuse {
		return false, ErrInvalidResponse
	}

	return cmd == CmdOK, nil
}

type Pull struct {
	N int
}

func (p *Pull) String() string {
	return fmt.Sprintf("%s %d", CmdPull, p.N)
}

type Prepare struct {
	N int
}

func (p *Prepare) String() string {
	return fmt.Sprintf("%s %d", CmdPrepare, p.N)
}

type Promise struct {
	Promise  bool
	Previous bool
	N        int
	V        string
	ID       string
}

func (r *Response) Promise() (*Promise, error) {
	cmd, args := r.Cmd()
	if cmd != CmdPromise && cmd != CmdRefuse {
		return nil, ErrInvalidResponse
	}

	promise := &Promise{
		Promise: cmd == CmdPromise,
	}

	splitArgs := strings.Split(args, " ")
	if len(splitArgs) == 3 {
		previousN, err := strconv.Atoi(splitArgs[0])
		if err != nil {
			return nil, err
		}
		promise.N = previousN
		promise.ID = splitArgs[1]
		promise.V = splitArgs[2]
		promise.Previous = true
	}
	return promise, nil
}

type Accept struct {
	N  int
	V  string
	ID string
}

func (a *Accept) String() string {
	return fmt.Sprintf("%s %d %s %s", CmdAccept, a.N, a.ID, a.V)
}

type Accepted struct {
	Accepted bool
}

func (r *Response) Accepted() (*Accepted, error) {
	cmd, _ := r.Cmd()
	if cmd != CmdAccepted && cmd != CmdRefuse {
		return nil, ErrInvalidResponse
	}

	accepted := &Accepted{
		Accepted: cmd == CmdAccepted,
	}

	return accepted, nil
}

type Set struct {
	N  int
	ID string
	V  string
}

func (s *Set) String() string {
	return fmt.Sprintf("%s %d %s %s", CmdSet, s.N, s.ID, s.V)
}
