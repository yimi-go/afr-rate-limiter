package fair_queue

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_newQueueSet(t *testing.T) {
	qs := newQueueSet(128, 16)
	assert.NotNil(t, qs)
	assert.Len(t, qs.queues, 128)
	assert.Len(t, qs.priority, 128)
	for i := 0; i < 128; i++ {
		assert.Equal(t, 0, qs.queues[i].len)
		assert.Equal(t, 16, qs.queues[i].cap)
		assert.Equal(t, 0, qs.priority[i].len)
		assert.Equal(t, 16, qs.priority[i].cap)
		assert.Equal(t, i, qs.priority[i].heapPosition)
	}
}

func Test_queueSet_enqueue_pop(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	offset := rand.Intn(128)
	qs := newQueueSet(128, 16)
	assert.Equal(t, int64(math.MaxInt64), qs.priority[0].headQueueSetPosition())
	for i := 0; i < 1000; i++ {
		_, heapify := qs.queues[(offset+i)%128].tryEnqueue()
		if heapify {
			qs.switchAndMinHeapify(qs.queues[(offset+i)%128])
		}
		assert.Less(t, qs.priority[0].headQueueSetPosition(), int64(math.MaxInt64))
		for i := 0; i < 128; i++ {
			assert.Equal(t, i, qs.priority[i].heapPosition)
		}
	}
	for i := 0; i < 999; i++ {
		qn := qs.popFirst()
		assert.NotNil(t, qn)
		assert.Less(t, qs.priority[0].headQueueSetPosition(), int64(math.MaxInt64))
		for i := 0; i < 128; i++ {
			assert.Equal(t, i, qs.priority[i].heapPosition)
		}
	}
	qn := qs.popFirst()
	assert.NotNil(t, qn)
	assert.Equal(t, int64(math.MaxInt64), qs.priority[0].headQueueSetPosition())
	for i := 0; i < 128; i++ {
		assert.Equal(t, i, qs.priority[i].heapPosition)
	}
	assert.Nil(t, qs.popFirst())
}

func Test_queueSet_get(t *testing.T) {
	qs := newQueueSet(128, 16)
	for i := 0; i < 128; i++ {
		assert.Same(t, qs.queues[i], qs.get(i))
	}
	func() {
		defer func() { assert.NotNil(t, recover()) }()
		qs.get(-1)
	}()
	func() {
		defer func() { assert.NotNil(t, recover()) }()
		qs.get(128)
	}()
}
