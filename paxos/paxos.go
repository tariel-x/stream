package paxos

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"

	"github.com/satori/go.uuid"

	"github.com/tariel-x/whynot/client"
	"github.com/tariel-x/whynot/stream"
)

var (
	ErrQuorumFailed = errors.New("quorum failed")
)

type logger struct{}

func (l *logger) Println(message ...interface{}) {
	log.Println(message...)
}

type Paxos struct {
	*paxos
}

func NewPaxos(nodes []string, name string) (*Paxos, error) {
	wnpaxos, err := newPaxos(nodes, name)
	return &Paxos{
		paxos: wnpaxos,
	}, err
}

func (p *Paxos) Prepare(n int) (bool, stream.AcceptMessage) {
	accepted, acceptMessage := p.paxos.Prepare(n)
	if acceptMessage == nil {
		return accepted, nil
	}
	return accepted, stream.AcceptMessage(acceptMessage)
}

type paxos struct {
	nodes      []*client.Client
	minQuorum  int
	acceptedV  *string
	acceptedID *string
	n          *uint64
	setted     map[string]struct{}
	settedM    sync.RWMutex
}

func newPaxos(nodes []string, name string) (*paxos, error) {
	clients := []*client.Client{}
	for _, node := range nodes {
		client, err := client.New(node, nil)
		client.SetName(name)
		client.Logger = &logger{}
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	minQuorum := (len(nodes) / 2) + 1
	startN := uint64(0)
	p := &paxos{
		nodes:     clients,
		minQuorum: minQuorum,
		n:         &startN,
		setted:    map[string]struct{}{},
		settedM:   sync.RWMutex{},
	}
	atomic.StoreUint64(p.n, 0)
	return p, nil
}

type AcceptMessage struct {
	n  uint64
	id string
	v  string
}

func (am *AcceptMessage) N() int {
	return int(am.n)
}
func (am *AcceptMessage) ID() string {
	return am.id
}
func (am *AcceptMessage) V() string {
	return am.v
}

func (p *paxos) Set(id string) {
	p.settedM.Lock()
	defer p.settedM.Unlock()
	p.setted[id] = struct{}{}
}

func (p *paxos) getSetted(id string) bool {
	p.settedM.RLock()
	defer p.settedM.RUnlock()
	_, ok := p.setted[id]
	if ok {
		delete(p.setted, id)
	}
	return ok
}

func (p *paxos) Commit(v string) (int, error) {
	var acceptMessage *AcceptMessage
	var err error

	id := uuid.NewV4().String()

	//TODO: if the foreign value is commited by the origin node - skip value.
	for acceptMessage == nil || (acceptMessage != nil && acceptMessage.id != id) {
		acceptMessage, err = p.commit(v, id)
		if err != nil {
			return 0, err
		}
		// Inc N counter to make the next proposition.
		atomic.AddUint64(p.n, 1)
	}
	return acceptMessage.N(), nil
}

func (p *paxos) commit(v, id string) (*AcceptMessage, error) {
	var acceptMessage *AcceptMessage
	var err error

commitCycle:
	for {
	promisePhase:
		for {
			acceptMessage, err = p.prepare(atomic.LoadUint64(p.n), v, id)
			switch err {
			case nil:
				break promisePhase
			case ErrQuorumFailed:
				atomic.AddUint64(p.n, 2) //TODO: set max proposed N in quorum + 1
			default:
				return nil, err
			}
		}

		// If the returned from the node elder proposed message is already set than skip it.
		if p.getSetted(acceptMessage.id) {
			return nil, nil
		}
		// Accept phase
		err = p.accept(acceptMessage)
		switch err {
		case nil:
			break commitCycle
		case ErrQuorumFailed:
			atomic.AddUint64(p.n, 2) //TODO: set max proposed N in quorum + 1
		default:
			return nil, err
		}
	}
	return acceptMessage, p.set(acceptMessage)
}

//Prepare returns true if proposed N is more than last known N.
//If some value is accepted but not set, it would be also returned.
func (p *paxos) Prepare(n int) (bool, *AcceptMessage) {
	if n > int(atomic.LoadUint64(p.n)) {
		var msg *AcceptMessage
		if p.acceptedV != nil {
			msg = &AcceptMessage{
				n:  atomic.LoadUint64(p.n),
				id: *p.acceptedID,
				v:  *p.acceptedV,
			}
		}
		atomic.StoreUint64(p.n, uint64(n))
		p.acceptedV = nil
		p.acceptedID = nil
		return true, msg
	}
	return false, nil
}

func (p *paxos) Accept(n int, v, id string) bool {
	if n >= int(atomic.LoadUint64(p.n)) {
		atomic.StoreUint64(p.n, uint64(n))
		p.acceptedV = &v
		p.acceptedID = &id
		return true
	}
	return false
}

func (p *paxos) prepare(n uint64, v, id string) (*AcceptMessage, error) {
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
		if promise.Previous && promise.N < int(n) {
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

func (p *paxos) sendPrepare(nodeClient *client.Client, wg *sync.WaitGroup, promises chan client.Promise, n uint64) {
	defer wg.Done()

	response, err := nodeClient.QueryOne(&client.Prepare{N: int(n)})
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

func (p *paxos) accept(message *AcceptMessage) error {
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

func (p *paxos) sendAccept(nodeClient *client.Client, wg *sync.WaitGroup, accepts chan client.Accepted, n uint64, v, id string) {
	defer wg.Done()
	response, err := nodeClient.QueryOne(&client.Accept{
		N:  int(n),
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

func (p *paxos) set(message *AcceptMessage) error {
	setRequest := &client.Set{
		N:  int(message.n),
		ID: message.id,
		V:  message.v,
	}
	for _, node := range p.nodes {
		go node.Exec(setRequest)
	}
	return nil
}
