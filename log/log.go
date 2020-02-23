package log

import (
	"context"
	"errors"
	"sync"
)

type item struct {
	n        int
	v        string
	next     *item
	previous *item
}

type Log struct {
	first *item
	last  *item
	m     *sync.RWMutex
}

func NewLog() (*Log, error) {
	return &Log{
		m: &sync.RWMutex{},
	}, nil
}

func (l *Log) Set(ctx context.Context, n int, v string) error {
	l.m.Lock()
	defer l.m.Unlock()
	if l.first == nil || l.last == nil {
		l.init(n, v)
		return nil
	}

	// Search correct position.
	cursor := l.last
	for cursor.n >= n {
		cursor = cursor.previous
	}
	if l.last == cursor && cursor.next == nil {
		l.append(n, v)
		return nil
	}
	l.insert(cursor, cursor.next, n, v)
	return nil
}

func (l *Log) init(n int, v string) {
	new := &item{
		n:        n,
		v:        v,
		next:     nil,
		previous: nil,
	}
	l.first = new
	l.last = new
}

func (l *Log) append(n int, v string) {
	current := l.last
	new := &item{
		n:        n,
		v:        v,
		next:     nil,
		previous: current,
	}
	current.next = new
	l.last = new
}

func (l *Log) insert(left, right *item, n int, v string) {
	new := &item{
		n:        n,
		v:        v,
		next:     right,
		previous: left,
	}
	if left != nil {
		left.next = new
	}
	if right != nil {
		right.previous = new
	}
}

func (l *Log) Pull(ctx context.Context, n int) (chan string, error) {
	if n < 0 {
		return nil, errors.New("invalid n")
	}
	results := make(chan string)
	cursor := l.first
	for cursor.n < n {
		cursor = cursor.next
	}
	go func() {
		l.m.RLock()
		defer l.m.RUnlock()
		defer close(results)
		for cursor != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if cursor.n < n {
				continue
			}
			results <- cursor.v
			cursor = cursor.next
		}
	}()

	return results, nil
}
