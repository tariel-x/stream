package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrUnknownCmd   = errors.New("unknown cmd")
	ErrIncorrectCmd = errors.New("incorrect cmd")

	ResponseOK = "ok"

	InvalidN = -1
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
		i++
	}
	i = 0
	wg.Wait()
	close(nodeResponses)
	for nodeN := range nodeResponses {
		log.Println(nodeN)
		i++
	}

	return nil
}

func (wn *Whynot) sendPrepareToNode(node string, wg *sync.WaitGroup, nodeResponses chan int) {
	defer wg.Done()
	conn, err := net.Dial("tcp", node)
	if err != nil {
		log.Println(err)
		return
	}

	if _, err := fmt.Fprintf(conn, "PREPARE"+strconv.Itoa(wn.N)+"\n"); err != nil {
		log.Println(err)
		return
	}

	log.Println(node, "->", wn.N)
	nodeResponse, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(node, "<-", node)
	responseN, err := strconv.Atoi(strings.TrimSpace(string(nodeResponse)))
	if err != nil {
		log.Println(nodeResponse)
		log.Println(err)
	}

	nodeResponses <- responseN
}

func (wn *Whynot) status() (string, error) {
	return "status ok", nil
}
