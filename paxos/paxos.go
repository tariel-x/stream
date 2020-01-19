package paxos

import (
	"errors"
	"log"
	"sync"

	"github.com/satori/go.uuid"
	"github.com/tariel-x/whynot/client"
)

var (
	ErrQuorumFailed = errors.New("quorum failed")
)

type WnPaxos struct {
	nodes      []*client.Client
	minQuorum  int
	N          int
	acceptedV  *string
	acceptedID *string
}

type Paxos struct {
	*WnPaxos
}

func NewPaxos(nodes []string) (*Paxos, error) {
	wnpaxos, err := NewWnPaxos(nodes)
	return &Paxos{wnpaxos}, err
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
	minQuorum := (len(nodes) / 2) + 1
	return &WnPaxos{
		nodes:     clients,
		N:         0,
		minQuorum: minQuorum,
	}, nil
}

type AcceptMessage struct {
	n  int
	id string
	v  string
}

func (am *AcceptMessage) N() int {
	return am.n
}
func (am *AcceptMessage) ID() string {
	return am.id
}
func (am *AcceptMessage) V() string {
	return am.v
}

func (p *WnPaxos) Commit(v string) (int, error) {
	var acceptMessage *AcceptMessage
	var err error

	id := uuid.NewV4().String()

	for acceptMessage == nil || (acceptMessage != nil && acceptMessage.id != id) {
		acceptMessage, err = p.commit(p.N, v, id)
		if err != nil {
			return 0, err
		}
		// Inc N counter to make the next proposition.
		p.N = acceptMessage.n + 1
	}
	return acceptMessage.n, nil
}

func (p *WnPaxos) commit(n int, v, id string) (*AcceptMessage, error) {
	var acceptMessage *AcceptMessage
	var err error

commitCycle:
	for {
	promisePhase:
		for {
			acceptMessage, err = p.prepare(n, v, id)
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

//Prepare returns true if proposed N is more than last known N.
//If some value is accepted but not set, it would be also returned.
func (p *WnPaxos) Prepare(n int) (bool, *AcceptMessage) {
	if n > p.N {
		var msg *AcceptMessage
		if p.acceptedV != nil {
			msg = &AcceptMessage{
				n:  p.N,
				id: *p.acceptedID,
				v:  *p.acceptedV,
			}
		}
		p.N = n
		p.acceptedV = nil
		p.acceptedID = nil
		return true, msg
	}
	return false, nil
}

func (p *WnPaxos) Accept(n int, v, id string) bool {
	if n >= p.N {
		p.N = n
		p.acceptedV = &v
		p.acceptedID = &id
		return true
	}
	return false
}

func (p *WnPaxos) prepare(n int, v, id string) (*AcceptMessage, error) {
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
	acceptMessage := &AcceptMessage{
		n:  n,
		id: id,
		v:  v,
	}
	rejection := false

	for promise := range promises {
		if !promise.Promise {
			rejection = true
			break
		}
		count++
		if promise.Previous && promise.N < n {
			if promise.N > maxPrevPromisedN {
				acceptMessage.v = promise.V
				acceptMessage.id = promise.ID
			}
		}
	}

	if count < p.minQuorum || rejection {
		return nil, ErrQuorumFailed
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

func (p *WnPaxos) accept(message *AcceptMessage) error {
	wg := &sync.WaitGroup{}
	accepts := make(chan client.Accepted, len(p.nodes))
	for _, node := range p.nodes {
		wg.Add(1)
		go p.sendAccept(node, wg, accepts, message.n, message.v, message.id)
	}

	wg.Wait()
	close(accepts)
	count := 0
	rejection := false

	for accept := range accepts {
		if !accept.Accepted {
			rejection = true
			break
		}
		count++
	}

	if count < p.minQuorum || rejection {
		return ErrQuorumFailed
	}
	return nil
}

func (p *WnPaxos) sendAccept(nodeClient *client.Client, wg *sync.WaitGroup, accepts chan client.Accepted, n int, v, id string) {
	defer wg.Done()
	response, err := nodeClient.QueryOne(&client.Accept{
		N:  n,
		V:  v,
		ID: id,
	})
	if err != nil {
		log.Println(err)
		return
	}

	agreement, err := response.Accepted()
	if err != nil {
		log.Println("can not parse reply", err)
		return
	}
	if agreement != nil {
		accepts <- *agreement
	}
}

func (p *WnPaxos) set(message *AcceptMessage) error {
	setRequest := &client.Set{
		N: message.n,
		V: message.v,
	}
	for _, node := range p.nodes {
		go node.Exec(setRequest)
	}
	return nil
}
