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
	"time"
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

func (wn *Whynot) Process(command string, results chan string) error {
	parsed := strings.SplitN(command, " ", 2)
	if len(parsed) == 0 {
		return ErrIncorrectCmd
	}
	switch parsed[0] {
	case "PUSH":
		if len(parsed) < 2 {
			return ErrIncorrectCmd
		}
		return wn.push(parsed[1], results)
	case "PULL":
		for i := 0; i < 5; i++ {
			results <- strconv.Itoa(i)
			time.Sleep(time.Millisecond * 300)
		}
		return nil
	case "STATUS":
		return wn.status(results)
	default:
		return ErrUnknownCmd
	}
}

func (wn *Whynot) push(args string, results chan string) error {
	results <- ResponseOK
	return nil
}

func (wn *Whynot) prepare(results chan string) error {
	wg := &sync.WaitGroup{}
	nodeResponses := make(chan int, len(wn.nodesList))
	for _, node := range wn.nodesList {
		wg.Add(1)
		go wn.sendPrepareToNode(node, wg, nodeResponses)
	}
	i := 0
	minQuorum := (len(wn.nodesList) / 2) - 1
	wg.Wait()
	close(nodeResponses)
	for nodeN := range nodeResponses {
		log.Println(nodeN)
		i++
		if i > minQuorum {
			break //TODO: remove break here and add checking of the quorum after reading all results
		}
	}

	return nil
}

func (wn *Whynot) sendPrepareToNode(node string, wg *sync.WaitGroup, nodeResponses chan int) {
	defer wg.Done()
	conn, err := net.DialTimeout("tcp", node, time.Second)
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

func (wn *Whynot) status(results chan string) error {
	results <- "status ok"
	return nil
}
