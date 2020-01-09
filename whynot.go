package main

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tariel-x/whynot/client"
)

var (
	ErrUnknownCmd   = errors.New("unknown cmd")
	ErrIncorrectCmd = errors.New("incorrect cmd")

	ResponseOK = newResponse("ok")

	InvalidN = -1
)

type Whynot struct {
	paxos *WnPaxos
}

func NewWhynot(paxos *WnPaxos) (*Whynot, error) {
	return &Whynot{
		paxos: paxos,
	}, nil
}

func (wn *Whynot) Process(message Request, results chan *Response) error {
	cmd, _, err := parse(message.Message)
	if err != nil {
		return err
	}
	switch cmd {
	case client.CmdPush:
		return wn.push(cmd, results)
	case client.CmdPull:
		for i := 0; i < 5; i++ {
			results <- newResponse(strconv.Itoa(i))
			time.Sleep(time.Millisecond * 300)
		}
		return nil
	case client.CmdStatus:
		return wn.status(results)
	default:
		return ErrUnknownCmd
	}
}

func (wn *Whynot) push(args string, results chan *Response) error {
	results <- ResponseOK
	return nil
}

func (wn *Whynot) status(results chan *Response) error {
	results <- newResponse("status ok")
	return nil
}

func parse(message string) (string, string, error) {
	parsed := strings.SplitN(message, " ", 2)
	if len(parsed) == 0 {
		return "", "", ErrIncorrectCmd
	}
	switch parsed[0] {
	case client.CmdPush:
		if len(parsed) < 2 {
			return "", "", ErrIncorrectCmd
		}
		return parsed[0], parsed[1], nil
	case client.CmdPull:
		return parsed[0], "", nil
	case client.CmdStatus:
		return parsed[0], "", nil
	case client.CmdPrepare:
		if len(parsed) < 2 {
			return "", "", ErrIncorrectCmd
		}
		return parsed[0], parsed[1], nil
	default:
		return "", "", ErrUnknownCmd
	}
}
