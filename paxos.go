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
	nodes     []*client.Client
	N         int
	AcceptedV *string
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

//Prepare returns true if proposed N is more than last known N.
//If some value is accepted but not set, it would be also returned.
func (p *WnPaxos) Prepare(n int) (bool, *AcceptMessage) {
	if n > p.N {
		var msg *AcceptMessage
		if p.AcceptedV != nil {
			msg.N = p.N
			msg.V = p.AcceptedV
		}
		p.N = n
		return true, msg
	}
	return false, nil
}

func (p *WnPaxos) Accept(n int, value string) bool {
	if n >= p.N {
		p.N = n
		p.AcceptedV = &value
		return true
	}
	return false
}

func (p *WnPaxos) Commit(v string) error {
	acceptMessage := &AcceptMessage{}
	var err error

	for acceptMessage.V == nil || (acceptMessage.V != nil && *acceptMessage.V != v) {
		acceptMessage, err = p.commit(p.N, v)
		if err != nil {
			return err
		}
		// Inc N counter to make the next proposition.
		p.N = acceptMessage.N + 1
	}
	return nil
}

func (p *WnPaxos) commit(n int, v string) (*AcceptMessage, error) {
	var acceptMessage *AcceptMessage
	var err error

commitCycle:
	for {
	promisePhase:
		for {
			acceptMessage, err = p.prepare(n, v)
			switch err {
			case nil:
				break promisePhase
			case ErrQuorumFailed:
				n += 2 //TODO: set max proposed N in quorum + 1
			default:
				return nil, err
			}
		}
		// Accept phase
		err = p.accept(acceptMessage)
		switch err {
		case nil:
			break commitCycle
		case ErrQuorumFailed:
			n += 2 //TODO: set max proposed N in quorum + 1
		default:
			return nil, err
		}
	}
	return acceptMessage, p.set(acceptMessage)
}

type AcceptMessage struct {
	N int
	V *string
}

func (p *WnPaxos) prepare(n int, v string) (*AcceptMessage, error) {
	wg := &sync.WaitGroup{}
	promises := make(chan client.Promise, len(p.nodes))
	for _, node := range p.nodes {
		wg.Add(1)
		go p.sendPrepare(node, wg, promises, n)
	}

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
		if promise.Previous && promise.N < n {
			if promise.N > maxPrevPromisedN {
				maxPrevPromisedN = promise.N
				maxPrevPromisedV = promise.V
			}
		}
	}

	minQuorum := (len(p.nodes) / 2) + 1
	if count < minQuorum {
		return nil, ErrQuorumFailed
	}
	acceptMessage := &AcceptMessage{
		N: n,
		V: &v,
	}

	if maxPrevPromisedN > 0 {
		acceptMessage.V = &maxPrevPromisedV
	}
	return acceptMessage, nil
}

func (p *WnPaxos) sendPrepare(nodeClient *client.Client, wg *sync.WaitGroup, promises chan client.Promise, n int) {
	defer wg.Done()

	response, err := nodeClient.QueryOne(&client.Prepare{N: n})
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

func (p *WnPaxos) set(message *AcceptMessage) error {
	setRequest := &client.Set{
		N: message.N,
		V: *message.V,
	}
	for _, node := range p.nodes {
		go node.Exec(setRequest)
	}
	return nil
}

func (p *WnPaxos) accept(message *AcceptMessage) error {
	wg := &sync.WaitGroup{}
	accepts := make(chan client.Accepted, len(p.nodes))
	for _, node := range p.nodes {
		wg.Add(1)
		go p.sendAccept(node, wg, accepts, message.N, *message.V)
	}

	wg.Wait()
	close(accepts)
	count := 0

	for accept := range accepts {
		if !accept.Accepted {
			continue
		}
		count++
	}

	minQuorum := (len(p.nodes) / 2) + 1
	if count < minQuorum {
		return ErrQuorumFailed
	}
	return nil
}

func (p *WnPaxos) sendAccept(nodeClient *client.Client, wg *sync.WaitGroup, accepts chan client.Accepted, n int, v string) {
	defer wg.Done()
}
