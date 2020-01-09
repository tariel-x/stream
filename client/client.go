package client

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	CmdPush    = "PUSH"
	CmdPull    = "PULL"
	CmdStatus  = "STATUS"
	CmdPrepare = "PREPARE"
	CmdPromise = "PROMISE"
	CmdRefuse  = "REFUSE"
)

var (
	ErrInvalidResponse = errors.New("invalid response")
)

type Logger interface {
	Println(message ...string)
}

type NullLogger struct{}

func (nl *NullLogger) Println(message ...string) {}

type Client struct {
	Address string
	Timeout time.Duration
	Logger  Logger
}

func New(address string, timeout *time.Duration) (*Client, error) {
	client := &Client{
		Address: address,
		Timeout: time.Second,
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
	c.Client.Logger.Println("request to", c.Client.Address, message)
	_, err := fmt.Fprint(c.connection, message+"\n")
	return err
}

func (c *Connection) QueryOne(r Request) (*Response, error) {
	message := r.String()
	c.Client.Logger.Println("request to", c.Client.Address, message)
	if _, err := fmt.Fprint(c.connection, message+"\n"); err != nil {
		return nil, err
	}
	nodeResponse, err := bufio.NewReader(c.connection).ReadString('\n')
	if err != nil {
		return nil, err
	}
	log.Println("response from", c.Client.Address, nodeResponse)
	return &Response{Message: nodeResponse}, nil
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

type Prepare struct {
	N int
}

func (p *Prepare) String() string {
	return fmt.Sprintf("%s %d", CmdPrepare, p.N)
}

type Response struct {
	Message string
}

func (r *Response) Cmd() (string, string) {
	parsed := strings.SplitN(r.Message, " ", 2)
	if len(parsed) == 0 {
		return "", ""
	}
	return parsed[0], parsed[1]
}

type Promise struct {
	Promise  bool
	Previous bool
	N        int
	V        string
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
	if len(splitArgs) == 2 {
		previousN, err := strconv.Atoi(splitArgs[0])
		if err != nil {
			return nil, err
		}
		promise.N = previousN
		promise.V = splitArgs[1]
		promise.Previous = true
	}
	return promise, nil
}
