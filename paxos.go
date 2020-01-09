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
	ErrQuorumFailed = errors.New("quorum failed")
)

type WnPaxos struct {
	nodesList []string
	N         int
}

type NodeResponse struct {
	*Response
	PrevAccept *AcceptMessage
	Promise    bool
}

type AcceptMessage struct {
	N int
	V string
}

//TODO: make Accept function

func (p *WnPaxos) Prepare(value string) (*AcceptMessage, error) {
	wg := &sync.WaitGroup{}
	nodeResponses := make(chan NodeResponse, len(p.nodesList))
	for _, node := range p.nodesList {
		wg.Add(1)
		go p.sendPrepareToNode(node, wg, nodeResponses)
	}
	minQuorum := (len(p.nodesList) / 2) - 1
	wg.Wait()
	close(nodeResponses)
	count := 0
	var maxPrevPromisedN int
	var maxPrevPromisedV string

	for nodeResponse := range nodeResponses {
		if !nodeResponse.Promise {
			continue
		}
		count++
		if nodeResponse.PrevAccept != nil && nodeResponse.PrevAccept.N < p.N {
			if nodeResponse.PrevAccept.N > maxPrevPromisedN {
				maxPrevPromisedN = nodeResponse.PrevAccept.N
				maxPrevPromisedV = nodeResponse.PrevAccept.V
			}
		}
	}
	if count < minQuorum {
		return nil, ErrQuorumFailed
	}
	acceptMessage := &AcceptMessage{
		N: p.N,
		V: value,
	}

	if maxPrevPromisedN > 0 {
		acceptMessage.V = maxPrevPromisedV
	}
	return acceptMessage, nil
}

func (p *WnPaxos) sendPrepareToNode(node string, wg *sync.WaitGroup, nodeResponses chan NodeResponse) {
	defer wg.Done()
	input := fmt.Sprintf("%s %d\n", CmdPrepare, p.N)
	request, err := makeRequest(input, node)
	if err != nil {
		log.Println(err)
		return
	}
	response, err := p.requestNode(request)
	if err != nil {
		log.Println(err)
		return
	}

	cmd, args, err := parse(response.Message)
	if err != nil {
		log.Println(err)
		return
	}

	nodeResponse := NodeResponse{}
	if cmd == CmdPromise {
		nodeResponse.Promise = true
	}

	splitArgs := strings.Split(args, " ")
	if len(splitArgs) == 2 {
		previousN, err := strconv.Atoi(splitArgs[0])
		if err != nil {
			log.Println(err)
			return
		}
		nodeResponse.PrevAccept = &AcceptMessage{
			N: previousN,
			V: splitArgs[1],
		}
	}

	nodeResponses <- nodeResponse
}

func (p *WnPaxos) requestNode(message *Request) (*Response, error) {
	conn, err := net.DialTimeout("tcp", message.Address, time.Second)
	if err != nil {
		return nil, err
	}

	if _, err := fmt.Fprint(conn, message.Message+"\n"); err != nil {
		return nil, err
	}

	log.Println("this ->", message.Address, message.Message)

	nodeResponse, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return nil, err
	}
	log.Println("this, <-", message.Address, nodeResponse)
	return newResponse(nodeResponse), nil
}
