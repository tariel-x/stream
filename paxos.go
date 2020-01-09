package main

import (
	"errors"
	"log"
	"sync"

	"github.com/tariel-x/whynot/client"
)

var (
	ErrQuorumFailed = errors.New("quorum failed")
)

type WnPaxos struct {
	nodes []*client.Client
	N     int
}

func NewWnPaxos(nodes []string) (*WnPaxos, error) {
	clients := []*client.Client{}
	for _, node := range nodes {
		client, err := client.New(node, nil)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	return &WnPaxos{
		nodes: clients,
		N:     0,
	}, nil
}

type AcceptMessage struct {
	N int
	V string
}

//TODO: make Accept function

func (p *WnPaxos) Prepare(value string) (*AcceptMessage, error) {
	wg := &sync.WaitGroup{}
	promises := make(chan client.Promise, len(p.nodes))
	for _, node := range p.nodes {
		wg.Add(1)
		go p.prepare(node, wg, promises)
	}
	minQuorum := (len(p.nodes) / 2) - 1
	wg.Wait()
	close(promises)
	count := 0
	var maxPrevPromisedN int
	var maxPrevPromisedV string

	for promise := range promises {
		if !promise.Promise {
			continue
		}
		count++
		if promise.Previous && promise.N < p.N {
			if promise.N > maxPrevPromisedN {
				maxPrevPromisedN = promise.N
				maxPrevPromisedV = promise.V
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

func (p *WnPaxos) prepare(nodeClient *client.Client, wg *sync.WaitGroup, promises chan client.Promise) {
	defer wg.Done()

	response, err := nodeClient.QueryOne(&client.Prepare{N: p.N})
	if err != nil {
		log.Println(err)
		return
	}

	promise, err := response.Promise()
	if err != nil {
		log.Println("can not parse reply", err)
		return
	}
	if promise != nil {
		promises <- *promise
	}
}
