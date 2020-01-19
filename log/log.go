package log

import (
	"context"
	"errors"
	"strconv"
	"sync"
)

type item struct {
	n int
	v string
}

type Log struct {
	log []item
	m   *sync.RWMutex
}

func NewLog() (*Log, error) {
	dumnMessages := []item{}
	for i := 0; i < 10; i++ {
		dumnMessages = append(dumnMessages, item{
			n: i,
			v: strconv.Itoa(i),
		})
	}

	return &Log{
		log: dumnMessages,
		m:   &sync.RWMutex{},
	}, nil
}

func (l *Log) Set(ctx context.Context, n int, v string) error {
	l.m.Lock()
	defer l.m.Unlock()
	l.log = append(l.log, item{
		n: n,
		v: v,
	})
	return nil
}

func (l *Log) Pull(ctx context.Context, n int) (chan string, error) {
	results := make(chan string)

	if n < 0 {
		return nil, errors.New("invalid n")
	}
	go func() {
		l.m.RLock()
		defer l.m.RUnlock()
		defer close(results)
		for _, item := range l.log {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if item.n < n {
				continue
			}
			results <- item.v
		}
	}()

	return results, nil
}
