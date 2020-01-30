package stream

import (
	"fmt"
	"time"

	"github.com/tariel-x/whynot/client"
)

func (h *Handler) Push(request *PushRequest, response ServerResponse) error {
	n, err := h.paxos.Commit(request.v)
	if err != nil {
		return err
	}
	if err := h.log.Set(request.ctx, n, request.v); err != nil {
		return err
	}
	response.Push(client.CmdOK)
	return nil
}

func (h *Handler) Set(request *SetRequest, response ServerResponse) error {
	if err := h.log.Set(request.ctx, request.n, request.v); err != nil {
		return err
	}
	h.paxos.Set(request.id)
	response.Push(client.CmdOK)
	return nil
}

func (h *Handler) Status(response ServerResponse) error {
	response.Push(client.CmdOK)
	return nil
}

func (h *Handler) Pull(request PullRequest, response ServerResponse) error {
	results, err := h.log.Pull(request.ctx, request.n)
	if err != nil {
		return err
	}
readCycle:
	for {
		select {
		case <-request.ctx.Done():
			return nil
		case result, ok := <-results:
			if !ok {
				break readCycle
			}
			response.Push(result)
			time.Sleep(time.Millisecond * 300)
		}
	}
	return nil
}

func (h *Handler) Accept(request *AcceptRequest, response ServerResponse) error {
	if h.paxos.Accept(request.n, request.v, request.id) {
		response.Push(client.CmdAccepted)
	} else {
		response.Push(client.CmdRefuse)
	}
	return nil
}

func (h *Handler) Prepare(request *PrepareRequest, response ServerResponse) error {
	agreement, previousAccepted := h.paxos.Prepare(request.n)

	decision := client.CmdPromise

	if !agreement {
		decision = client.CmdRefuse
	}

	if previousAccepted == nil {
		response.Push(decision)
	} else {
		response.Push(fmt.Sprintf("%s %d %s %s", decision, previousAccepted.N(), previousAccepted.ID(), previousAccepted.V()))
	}

	return nil
}
