package main

import (
	"errors"
	"strings"
	"sync"
)

var (
	ErrUnknownCmd   = errors.New("unknown cmd")
	ErrIncorrectCmd = errors.New("incorrect cmd")

	ResponseOK = "ok"
)

type Whynot struct {
	nodesList []string
	N         int
}

func NewWhynot(nodesList []string) (*Whynot, error) {
	return &Whynot{
		nodesList: nodesList,
	}, nil
}

func (wn *Whynot) Process(command string) (string, error) {
	parsed := strings.SplitN(command, " ", 2)
	if len(parsed) == 0 {
		return "", ErrIncorrectCmd
	}
	switch parsed[0] {
	case "PUSH":
		if len(parsed) < 2 {
			return "", ErrIncorrectCmd
		}
		return wn.push(parsed[1])
	case "STATUS":
		return wn.status()
	default:
		return "", ErrUnknownCmd
	}
}

func (wn *Whynot) push(args string) (string, error) {

	return ResponseOK, nil
}

func (wn *Whynot) prepare() error {
	i := 0
	minQuorum := (len(wn.nodesList) / 2) - 1

	wg := &sync.WaitGroup{}
	nodeResponses := make(chan int, minQuorum)
	for _, node := range wn.nodesList {
		if i > minQuorum {
			break
		}
		wg.Add(1)
		go wn.sendPrepareToNode(node, wg, nodeResponses)
	}
	wg.Wait()

	return nil
}

func (wn *Whynot) sendPrepareToNode(node string, wg *sync.WaitGroup, nodeResponses chan int) {
	defer wg.Done()
	nodeResponses <- 0
}

func (wn *Whynot) status() (string, error) {
	return "status ok", nil
}
