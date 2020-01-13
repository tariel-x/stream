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

	availableCmds = map[string]struct{}{
		client.CmdPush:     {},
		client.CmdPull:     {},
		client.CmdStatus:   {},
		client.CmdPrepare:  {},
		client.CmdPromise:  {},
		client.CmdRefuse:   {},
		client.CmdAccept:   {},
		client.CmdAccepted: {},
		client.CmdSet:      {},
	}
)

type Whynot struct {
	paxos *WnPaxos
}

func NewWhynot(paxos *WnPaxos) (*Whynot, error) {
	return &Whynot{
		paxos: paxos,
	}, nil
}

type WhynotRequest struct {
	cmd  string
	args []string
}

func (wn *Whynot) Process(message Request, results chan *Response) error {
	req, err := parse(message.Message)
	if err != nil {
		return err
	}
	switch req.cmd {
	case client.CmdPush:
		return wn.push(req.cmd, results)
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

//TODO: make separate models for all requests
func parse(message string) (*WhynotRequest, error) {
	parsed := strings.SplitN(message, " ", 2)
	if len(parsed) == 0 {
		return nil, ErrIncorrectCmd
	}
	cmd, rawArgs := parsed[0], parsed[1]

	if _, ok := availableCmds[cmd]; !ok {
		return nil, ErrIncorrectCmd
	}
	args := strings.Split(rawArgs, " ")
	req := &WhynotRequest{
		cmd:  cmd,
		args: args,
	}

	switch cmd {
	case client.CmdPush:
		if len(args) == 0 {
			return nil, ErrIncorrectCmd
		}
		return req, nil
	case client.CmdPull:
		return req, nil
	case client.CmdStatus:
		return req, nil
	case client.CmdPrepare:
		if len(args) == 2 {
			return nil, ErrIncorrectCmd
		}
		return req, nil
	default:
		return nil, ErrUnknownCmd
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
