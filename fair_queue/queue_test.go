package fair_queue

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_emptyQueue(t *testing.T) {
	q := emptyQueue(16, 33)
	assert.NotNil(t, q)
	assert.Same(t, q.dummy, q.dummy.next)
	assert.Same(t, q.dummy, q.dummy.prev)
	assert.Equal(t, 16, q.cap)
	assert.Equal(t, 33, q.heapPosition)
	assert.Equal(t, 0, q.len)
}

func Test_queue_tryEnqueue(t *testing.T) {
	tests := []struct {
		queue       func() *queue
		name        string
		wantN       bool
		wantHeapify bool
	}{
		{
			name: "full",
			queue: func() *queue {
				dummy := &queueNode{}
				dummy.prev, dummy.next = dummy, dummy
				return &queue{
					dummy: dummy,
					len:   16,
					cap:   16,
				}
			},
			wantN:       false,
			wantHeapify: false,
		},
		{
			name: "empty",
			queue: func() *queue {
				return emptyQueue(16, 0)
			},
			wantN:       true,
			wantHeapify: true,
		},
		{
			name: "not_empty",
			queue: func() *queue {
				q := emptyQueue(16, 0)
				q.tryEnqueue()
				return q
			},
			wantN:       true,
			wantHeapify: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.queue()
			gotN, gotHeapify := q.tryEnqueue()
			assert.Equal(t, tt.wantN, gotN != nil)
			assert.Equal(t, tt.wantHeapify, gotHeapify)
		})
	}
}

func Test_queue_length(t *testing.T) {
	q := emptyQueue(16, 0)
	assert.Equal(t, 0, q.length())
	q.tryEnqueue()
	assert.Equal(t, 1, q.length())
}

func Test_queue_dequeue(t *testing.T) {
	q := emptyQueue(16, 0)
	n, _ := q.tryEnqueue()
	q.dequeue(n)
	assert.Equal(t, 0, q.len)
	assert.Nil(t, n.q)
	assert.Same(t, q.dummy, q.dummy.next)
	assert.Same(t, q.dummy, q.dummy.prev)
	assert.Nil(t, n.prev)
	assert.Nil(t, n.next)
}

func Test_queue_pop(t *testing.T) {
	q := emptyQueue(16, 0)
	assert.Nil(t, q.pop())
	n1, _ := q.tryEnqueue()
	n2, _ := q.tryEnqueue()
	pop := q.pop()
	assert.NotNil(t, pop)
	assert.Equal(t, 1, q.len)
	assert.Same(t, n2, q.dummy.next)
	assert.Same(t, q.dummy, n2.prev)
	assert.Same(t, n1, pop)
	assert.Nil(t, pop.prev)
	assert.Nil(t, pop.next)
}

func Test_queue_headQueueSetPosition(t *testing.T) {
	q := emptyQueue(16, 0)
	assert.Equal(t, int64(math.MaxInt64), q.headQueueSetPosition())
	n, _ := q.tryEnqueue()
	n.queueSetPosition = 1
	assert.Equal(t, int64(1), q.headQueueSetPosition())
}
