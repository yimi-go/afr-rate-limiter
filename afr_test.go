package afr_rate_limiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	limiter "github.com/yimi-go/rate-limiter"
	"github.com/yimi-go/window"

	"github.com/yimi-go/afr-rate-limiter/stat"
)

func TestNew(t *testing.T) {
	l := New(stat.New())
	assert.NotNil(t, l)
}

func TestLimiter_Allow(t *testing.T) {
	t.Run("drop", func(t *testing.T) {
		l := New(stat.New(), WithDropper(DropFunc(func() bool {
			return true
		})))
		allow, err := l.Allow("")
		assert.True(t, limiter.IsErrLimitExceed(err))
		assert.Nil(t, allow)
	})
	t.Run("try_not_allowed", func(t *testing.T) {
		l := New(stat.New(), WithDropper(DropFunc(func() bool {
			return false
		})), WithTrier(TryFunc(func(_ string) (<-chan struct{}, func(canceled bool), bool) {
			return nil, nil, false
		})))
		allow, err := l.Allow("")
		assert.True(t, limiter.IsErrLimitExceed(err))
		assert.Nil(t, allow)
	})
	t.Run("allowed_ok", func(t *testing.T) {
		now := window.Now()
		window.Now = func() time.Time { return now }
		defer func() { window.Now = time.Now }()
		ch := make(chan struct{})
		close(ch)
		var canceled bool
		l := New(stat.New(), WithDropper(DropFunc(func() bool {
			return false
		})), WithTrier(TryFunc(func(_ string) (<-chan struct{}, func(bool), bool) {
			return ch, func(c bool) { canceled = c }, true
		})))
		allow, err := l.Allow("")
		assert.Nil(t, err)
		select {
		case <-allow.Ready():
		default:
			assert.Fail(t, "should ready")
		}
		assert.Equal(t, int64(1), l.stats.InFlight())
		now = now.Add(time.Second)
		allow.Done(limiter.StateOk, nil)
		assert.Equal(t, int64(time.Second), l.stats.RTStat().Aggregation(0).Sum())
		assert.Equal(t, int64(0), l.stats.InFlight())
		assert.Equal(t, int64(1), l.stats.PassStat().Aggregation(0).Sum())
		assert.Equal(t, int64(0), l.stats.TimeoutStat().Aggregation(0).Sum())
		assert.False(t, canceled)
	})
	t.Run("allowed_timeout", func(t *testing.T) {
		now := window.Now()
		window.Now = func() time.Time { return now }
		defer func() { window.Now = time.Now }()
		ch := make(chan struct{})
		close(ch)
		var canceled bool
		l := New(stat.New(), WithDropper(DropFunc(func() bool {
			return false
		})), WithTrier(TryFunc(func(_ string) (<-chan struct{}, func(bool), bool) {
			return ch, func(c bool) { canceled = c }, true
		})))
		allow, err := l.Allow("")
		assert.Nil(t, err)
		select {
		case <-allow.Ready():
		default:
			assert.Fail(t, "should ready")
		}
		assert.Equal(t, int64(1), l.stats.InFlight())
		now = now.Add(time.Second)
		allow.Done(limiter.StateTimeout, nil)
		assert.Equal(t, int64(time.Second), l.stats.RTStat().Aggregation(0).Sum())
		assert.Equal(t, int64(0), l.stats.InFlight())
		assert.Equal(t, int64(1), l.stats.PassStat().Aggregation(0).Sum())
		assert.Equal(t, int64(1), l.stats.TimeoutStat().Aggregation(0).Sum())
		assert.True(t, canceled)
	})
}

type DropFunc func() bool

func (f DropFunc) Drop() bool {
	return f()
}

type TryFunc func(id string) (ready <-chan struct{}, done func(canceled bool), allowed bool)

func (f TryFunc) Try(id string) (ready <-chan struct{}, done func(canceled bool), allowed bool) {
	return f(id)
}
