package stream

import (
	"time"

	"github.com/tariel-x/whynot/client"
)

func (h *Handler) Push(request PushRequest, response ServerResponse) error {
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

func (h *Handler) Set(request SetRequest, response ServerResponse) error {
	if err := h.log.Set(request.ctx, request.n, request.v); err != nil {
		return err
	}
	response.Push(client.CmdOK)
	return nil
}

func (h *Handler) Status(response ServerResponse) error {
	response.Push(client.CmdOK)
	return nil
}

func (h *Handler) Pull(request PullRequest, response ServerResponse) error {
	results := make(chan string)
	if err := h.log.Pull(request.ctx, request.n, results); err != nil {
		return err
	}
	defer close(results)
	for {
		select {
		case <-request.ctx.Done():
			return nil
		case result := <-results:
			response.Push(result)
			time.Sleep(time.Millisecond * 300)
		}
	}
	return nil
}

func (h *Handler) Accept(request AcceptRequest, response ServerResponse) error {
	return nil
}
