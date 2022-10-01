package fair_queue

import (
	"math"
	"sync"
)

type queueNode struct {
	q                *queue
	prev, next       *queueNode
	ready            chan struct{}
	queueSetPosition int64
}

type queue struct {
	dummy        *queueNode
	len          int
	cap          int
	heapPosition int
	mu           sync.RWMutex
}

func emptyQueue(cap, heapPosition int) *queue {
	dummy := &queueNode{}
	dummy.prev, dummy.next = dummy, dummy
	return &queue{
		dummy:        dummy,
		cap:          cap,
		heapPosition: heapPosition,
	}
}

func (q *queue) tryEnqueue() (n *queueNode, heapify bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.len == q.cap {
		return nil, false
	}
	n = &queueNode{
		q:     q,
		prev:  q.dummy.prev,
		next:  q.dummy,
		ready: make(chan struct{}),
	}
	q.dummy.prev.next, q.dummy.prev = n, n
	q.len++
	return n, q.len == 1
}

func (q *queue) length() (length int) {
	q.mu.RLock()
	length = q.len
	q.mu.RUnlock()
	return
}

func (q *queue) dequeue(n *queueNode) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n.q = nil
	n.prev.next, n.next.prev = n.next, n.prev
	n.next, n.prev = nil, nil
	q.len--
}

func (q *queue) pop() *queueNode {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.len == 0 {
		return nil
	}
	n := q.dummy.next
	q.dummy.next, n.next.prev = n.next, q.dummy
	n.prev, n.next = nil, nil
	q.len--
	return n
}

func (q *queue) headQueueSetPosition() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.len == 0 {
		return math.MaxInt64
	}
	return q.dummy.next.queueSetPosition
}
