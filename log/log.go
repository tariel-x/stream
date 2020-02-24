package log

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type item struct {
	n        int
	v        string
	next     *item
	previous *item
}

type wait struct {
	c      chan *item
	count  uint64
	border *item
}

type Log struct {
	first       *item
	last        *item
	m           sync.RWMutex
	count       uint64
	waitlist    map[uint64]wait
	connections *uint64
}

func NewLog() (*Log, error) {
	l := &Log{
		m:           sync.RWMutex{},
		waitlist:    map[uint64]wait{},
		connections: new(uint64),
	}
	atomic.StoreUint64(l.connections, 0)
	return l, nil
}

func (l *Log) removeWait(i uint64) {
	l.m.Lock()
	defer l.m.Unlock()
	delete(l.waitlist, i)
}

func (l *Log) addWait(w wait) uint64 {
	l.m.Lock()
	defer l.m.Unlock()
	i := atomic.AddUint64(l.connections, 1)
	l.waitlist[i] = w
	return i
}

func (l *Log) Set(ctx context.Context, n int, v string) error {
	l.m.Lock()
	defer l.m.Unlock()
	defer func() {
		for _, w := range l.waitlist {
			w.c <- l.last
		}
	}()
	l.count++
	if l.first == nil || l.last == nil {
		l.init(n, v)
		return nil
	}

	// Search correct position.
	cursor := l.last
	if l.last == nil {
	}
	for cursor.previous != nil && cursor.n >= n {
		cursor = cursor.previous
	}
	// Found element is the last.
	if l.last == cursor && cursor.next == nil {
		l.append(n, v)
		return nil
	}
	// Insert in the middle of the list.
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

func (l *Log) Get(ctx context.Context, n int) ([]string, error) {
	if n < 0 {
		return nil, errors.New("invalid n")
	}
	l.m.RLock()
	defer l.m.RUnlock()
	cursor := l.first
	if cursor == nil {
		return nil, nil
	}
	for cursor.n < n {
		cursor = cursor.next
	}
	var results []string
	for cursor != nil {
		select {
		case <-ctx.Done():
			return results, nil
		default:
		}
		if cursor.n < n {
			continue
		}
		results = append(results, cursor.v)
		cursor = cursor.next
	}

	return results, nil
}

func (l *Log) Pull(ctx context.Context, n int) (chan string, error) {
	if n < 0 {
		return nil, errors.New("invalid n")
	}
	w := wait{
		c:      make(chan *item, l.count),
		border: l.last,
	}
	thiswait := l.addWait(w)

	results := make(chan string)
	go func() {
		defer close(results)
		defer close(w.c)
		defer l.removeWait(thiswait)

		l.m.RLock()
		cursor := l.first
		for cursor != nil && cursor.n < n {
			cursor = cursor.next
		}

		alreadySent := map[int]struct{}{}
		for cursor != nil && cursor.n <= w.border.n {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if cursor.n < n {
				continue
			}
			results <- cursor.v
			alreadySent[cursor.n] = struct{}{}
			cursor = cursor.next
		}
		l.m.RUnlock()

		for {
			select {
			case <-ctx.Done():
				return
			case new, ok := <-w.c:
				if !ok {
					return
				}
				if _, ok := alreadySent[new.n]; ok {
					continue
				}
				results <- new.v
			}
		}
	}()

	return results, nil
}
