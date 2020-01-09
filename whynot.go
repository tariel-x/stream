package main

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

var (
	ErrUnknownCmd   = errors.New("unknown cmd")
	ErrIncorrectCmd = errors.New("incorrect cmd")

	ResponseOK = newResponse("ok")

	InvalidN = -1
)

const (
	CmdPush    = "PUSH"
	CmdPull    = "PULL"
	CmdStatus  = "STATUS"
	CmdPrepare = "PREPARE"
	CmdPromise = "PROMISE"
	CmdRefuse  = "REFUSE"
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
	case CmdPush:
		return wn.push(cmd, results)
	case CmdPull:
		for i := 0; i < 5; i++ {
			results <- newResponse(strconv.Itoa(i))
			time.Sleep(time.Millisecond * 300)
		}
		return nil
	case CmdStatus:
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
	case CmdPush:
		if len(parsed) < 2 {
			return "", "", ErrIncorrectCmd
		}
		return parsed[0], parsed[1], nil
	case CmdPull:
		return parsed[0], "", nil
	case CmdStatus:
		return parsed[0], "", nil
	case CmdPrepare:
		if len(parsed) < 2 {
			return "", "", ErrIncorrectCmd
		}
		return parsed[0], parsed[1], nil
	default:
		return "", "", ErrUnknownCmd
	}
}
