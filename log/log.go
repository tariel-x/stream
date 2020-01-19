package log

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

type Log struct {
	log []string
	m   *sync.RWMutex
}

func NewLog() (*Log, error) {
	dumnMessages := []string{}
	for i := 0; i < 10; i++ {
		dumnMessages = append(dumnMessages, strconv.Itoa(i))
		time.Sleep(time.Millisecond * 300)
	}

	return &Log{
		log: dumnMessages,
		m:   &sync.RWMutex{},
	}, nil
}

func (l *Log) Push(ctx context.Context, message string) error {
	l.m.Lock()
	defer l.m.Unlock()
	l.log = append(l.log, message)
	return nil
}

func (l *Log) Pull(ctx context.Context, n int, results chan string) error {
	l.m.RLock()
	defer l.m.RUnlock()
	if n > len(l.log) || n < 0 {
		return errors.New("invalid n")
	}
	for _, item := range l.log[n:] {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		results <- item
	}
	return nil
}
