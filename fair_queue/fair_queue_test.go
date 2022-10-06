package fair_queue

import (
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	limiter "github.com/yimi-go/rate-limiter"

	"github.com/yimi-go/afr-rate-limiter/stat"
)

func TestInit(t *testing.T) {
	select {
	case <-closedChan:
	default:
		t.Failed()
	}
}

func TestNewFairQueue(t *testing.T) {
	stats := stat.New()
	fq, err := NewFairQueue(stats)
	assert.Nil(t, err)
	assert.Same(t, stats, fq.stats)
	assert.Equal(t, 128, len(fq.set.(*queueSet).queues))
	assert.Equal(t, 32, fq.set.(*queueSet).queues[0].cap)
	assert.NotNil(t, fq.dealer)
	assert.Equal(t, int64(3000), fq.maxSeat)
	fq, err = NewFairQueue(stats,
		WithQueueNum(16),
		WithQueueCap(16),
		WithHandSize(16),
		WithInitSeat(16),
	)
	assert.NotNil(t, err)
	assert.Nil(t, fq)
}

func TestFairQueue_Try(t *testing.T) {
	t.Run("has_free_seat", func(t *testing.T) {
		fq, _ := NewFairQueue(stat.New())
		ready, done, allowed := fq.Try("")
		assert.True(t, allowed)
		select {
		case <-ready:
		default:
			assert.Fail(t, "should ready")
		}
		done(true)
	})
	t.Run("enqueue_ok", func(t *testing.T) {
		fq, _ := NewFairQueue(stat.New())
		fq.dealer = DealerFunc(func(hashValue uint64, hand []int) []int { return []int{1} })
		fq.busySeat = 3000
		ready, done, allowed := fq.Try("")
		assert.True(t, allowed)
		assert.Equal(t, 1, fq.set.(*queueSet).queues[1].len)
		assert.Less(t, fq.set.(*queueSet).queues[1].headQueueSetPosition(), int64(math.MaxInt64))
		assert.Same(t, fq.set.(*queueSet).queues[1], fq.set.(*queueSet).priority[0])
		assert.Equal(t, int64(3000), fq.busySeat)
		select {
		case <-ready:
			assert.Fail(t, "should not ready")
		default:
		}
		fq.onReqDone()
		assert.Equal(t, int64(3000), fq.busySeat)
		select {
		case <-ready:
		default:
			assert.Fail(t, "should ready")
		}
		done(false)
		assert.Equal(t, int64(2999), fq.busySeat)
	})
	t.Run("enqueue_timeout", func(t *testing.T) {
		fq, _ := NewFairQueue(stat.New())
		fq.dealer = DealerFunc(func(hashValue uint64, hand []int) []int { return []int{1} })
		fq.busySeat = 3000
		ready, done, _ := fq.Try("")
		_, _, _ = fq.Try("")
		assert.Equal(t, int64(3000), fq.busySeat)
		assert.Equal(t, 2, fq.set.(*queueSet).queues[1].len)
		assert.Same(t, fq.set.(*queueSet).queues[1], fq.set.(*queueSet).priority[0])
		select {
		case <-ready:
			assert.Fail(t, "should not ready")
		default:
		}
		done(true)
		assert.Equal(t, int64(3000), fq.busySeat)
		assert.Equal(t, 0, fq.set.(*queueSet).queues[1].len)
	})
	t.Run("all_full_and_has_timeout", func(t *testing.T) {
		stats := stat.New()
		stats.TimeoutStat().Add(1)
		fq, err := NewFairQueue(stats, WithQueueNum(8), WithQueueCap(1))
		assert.Nil(t, err)
		for i := range fq.set.(*queueSet).queues {
			_, _ = fq.set.(*queueSet).queues[i].tryEnqueue()
			qn, _ := fq.set.(*queueSet).queues[i].tryEnqueue()
			assert.Nil(t, qn)
		}
		fq.busySeat = 3000
		ready, _, allowed := fq.Try("")
		assert.False(t, allowed)
		select {
		case <-ready:
			assert.Fail(t, "should never ready")
		default:
		}
	})
	t.Run("increase_seat_ok_and_no_other_timeout", func(t *testing.T) {
		stats := stat.New()
		fq, err := NewFairQueue(stats, WithQueueNum(8), WithQueueCap(1))
		assert.Nil(t, err)
		for i := range fq.set.(*queueSet).queues {
			_, _ = fq.set.(*queueSet).queues[i].tryEnqueue()
			qn, _ := fq.set.(*queueSet).queues[i].tryEnqueue()
			assert.Nil(t, qn)
		}
		fq.busySeat = 3000
		ready, done, allowed := fq.Try("")
		assert.True(t, allowed)
		assert.Greater(t, fq.busySeat, int64(3000))
		select {
		case <-ready:
		default:
			assert.Fail(t, "should ready")
		}
		done(false)
		assert.Equal(t, int64(3001), atomic.LoadInt64(&fq.busySeat))
		assert.Equal(t, int64(3001), atomic.LoadInt64(&fq.maxSeat))
		assert.Equal(t, 0, fq.set.(*queueSet).priority[7].len)
	})
	t.Run("increase_seat_ok_but_other_timeout", func(t *testing.T) {
		stats := stat.New()
		fq, err := NewFairQueue(stats, WithQueueNum(8), WithQueueCap(1))
		assert.Nil(t, err)
		for i := range fq.set.(*queueSet).queues {
			_, _ = fq.set.(*queueSet).queues[i].tryEnqueue()
			qn, _ := fq.set.(*queueSet).queues[i].tryEnqueue()
			assert.Nil(t, qn)
		}
		fq.busySeat = 3000
		ready, done, allowed := fq.Try("")
		assert.True(t, allowed)
		assert.Greater(t, fq.busySeat, int64(3000))
		select {
		case <-ready:
		default:
			assert.Fail(t, "should ready")
		}
		fq.stats.TimeoutStat().Add(1)
		done(false)
		assert.Equal(t, int64(3000), atomic.LoadInt64(&fq.busySeat))
		assert.Equal(t, int64(3000), atomic.LoadInt64(&fq.maxSeat))
		assert.Equal(t, 1, fq.set.(*queueSet).priority[7].len)
	})
	t.Run("increase_seat_timeout", func(t *testing.T) {
		stats := stat.New()
		fq, err := NewFairQueue(stats, WithQueueNum(8), WithQueueCap(1))
		assert.Nil(t, err)
		for i := range fq.set.(*queueSet).queues {
			_, _ = fq.set.(*queueSet).queues[i].tryEnqueue()
			qn, _ := fq.set.(*queueSet).queues[i].tryEnqueue()
			assert.Nil(t, qn)
		}
		fq.busySeat = 3000
		ready, done, allowed := fq.Try("")
		assert.True(t, allowed)
		assert.Greater(t, fq.busySeat, int64(3000))
		select {
		case <-ready:
		default:
			assert.Fail(t, "should ready")
		}
		done(true)
		assert.Equal(t, int64(3000), atomic.LoadInt64(&fq.busySeat))
		assert.Equal(t, int64(3000), atomic.LoadInt64(&fq.maxSeat))
		assert.Equal(t, 1, fq.set.(*queueSet).priority[7].len)
	})
}

func TestFairQueue_Allow(t *testing.T) {
	t.Run("not_allow", func(t *testing.T) {
		stats := stat.New()
		stats.TimeoutStat().Add(1)
		fq, err := NewFairQueue(stats, WithQueueNum(8), WithQueueCap(1))
		assert.Nil(t, err)
		for i := range fq.set.(*queueSet).queues {
			_, _ = fq.set.(*queueSet).queues[i].tryEnqueue()
			qn, _ := fq.set.(*queueSet).queues[i].tryEnqueue()
			assert.Nil(t, qn)
		}
		fq.busySeat = 3000
		allow, err := fq.Allow("")
		assert.NotNil(t, err)
		assert.Nil(t, allow)
	})
	t.Run("allow_ok", func(t *testing.T) {
		now := Now()
		Now = func() time.Time { return now }
		defer func() { Now = time.Now }()
		fq, err := NewFairQueue(stat.New())
		assert.Nil(t, err)
		allow, err := fq.Allow("")
		assert.Nil(t, err)
		assert.NotNil(t, allow)
		assert.Equal(t, int64(1), fq.stats.InFlight())
		select {
		case <-allow.Ready():
		default:
			assert.Fail(t, "should ready")
		}
		now = now.Add(time.Second)
		allow.Done(limiter.StateOk, nil)
		assert.Equal(t, int64(time.Second), fq.stats.RTStat().Aggregation(0).Sum())
		assert.Equal(t, int64(0), fq.stats.InFlight())
		assert.Equal(t, int64(1), fq.stats.PassStat().Aggregation(0).Sum())
	})
	t.Run("allow_timeout", func(t *testing.T) {
		now := Now()
		Now = func() time.Time { return now }
		defer func() { Now = time.Now }()
		fq, err := NewFairQueue(stat.New())
		assert.Nil(t, err)
		allow, err := fq.Allow("")
		assert.Nil(t, err)
		assert.NotNil(t, allow)
		assert.Equal(t, int64(1), fq.stats.InFlight())
		select {
		case <-allow.Ready():
		default:
			assert.Fail(t, "should ready")
		}
		now = now.Add(time.Second)
		allow.Done(limiter.StateTimeout, nil)
		assert.Equal(t, int64(time.Second), fq.stats.RTStat().Aggregation(0).Sum())
		assert.Equal(t, int64(0), fq.stats.InFlight())
		assert.Equal(t, int64(1), fq.stats.PassStat().Aggregation(0).Sum())
		assert.Equal(t, int64(1), fq.stats.TimeoutStat().Aggregation(0).Sum())
	})
}

type DealerFunc func(hashValue uint64, hand []int) []int

func (f DealerFunc) DealIntoHand(hashValue uint64, hand []int) []int {
	return f(hashValue, hand)
}
