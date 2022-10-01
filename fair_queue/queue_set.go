package fair_queue

import "sync"

type queueSet struct {
	queues   []*queue
	priority []*queue
	mu       sync.RWMutex
}

func newQueueSet(queueNum, queueCap int) *queueSet {
	qs := &queueSet{
		queues:   make([]*queue, queueNum),
		priority: make([]*queue, queueNum),
	}
	for i := 0; i < queueNum; i++ {
		q := emptyQueue(queueCap, i)
		qs.queues[i], qs.priority[i] = q, q
	}
	return qs
}

func (s *queueSet) popFirst() *queueNode {
	s.mu.Lock()
	defer s.mu.Unlock()
	qn := s.priority[0].pop()
	if qn == nil {
		return nil
	}
	minHeapify(s.priority, 0, len(s.priority))
	for i := range s.priority {
		s.priority[i].heapPosition = i
	}
	return qn
}

func (s *queueSet) switchAndMinHeapify(q *queue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := q.heapPosition
	s.priority[0], s.priority[p] = s.priority[p], s.priority[0]
	minHeapify(s.priority, 0, len(s.priority))
	for i := range s.priority {
		s.priority[i].heapPosition = i
	}
}

func minHeapify(queues []*queue, i, heapSize int) {
	l, r, m := i<<1+1, i<<1+2, i
	if l < heapSize && queues[l].headQueueSetPosition() < queues[m].headQueueSetPosition() {
		m = l
	}
	if r < heapSize && queues[r].headQueueSetPosition() < queues[m].headQueueSetPosition() {
		m = r
	}
	if m != i {
		queues[i], queues[m] = queues[m], queues[i]
		minHeapify(queues, m, heapSize)
	}
}

func (s *queueSet) get(index int) *queue {
	return s.queues[index]
}
