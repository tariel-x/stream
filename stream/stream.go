package stream

import (
	"time"
)

func (h *Handler) Push(request PushRequest, response ServerResponse) error {
	response.Push("ok")
	return nil
}

func (h *Handler) Status(response ServerResponse) error {
	response.Push("status ok")
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
