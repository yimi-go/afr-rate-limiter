package afr_rate_limiter

import (
	"time"

	"github.com/yimi-go/rate-limiter"
	"github.com/yimi-go/window"

	"github.com/yimi-go/afr-rate-limiter/bbr"
	"github.com/yimi-go/afr-rate-limiter/fair_queue"
	"github.com/yimi-go/afr-rate-limiter/stat"
)

type Dropper interface {
	Drop() bool
}

type Trier interface {
	Try(id string) (ready <-chan struct{}, done func(canceled bool), allowed bool)
}

type Limiter struct {
	stats   stat.Stats
	dropper Dropper
	trier   Trier
}

type Option func(l *Limiter)

func WithDropper(d Dropper) Option {
	return func(l *Limiter) {
		l.dropper = d
	}
}

func WithTrier(t Trier) Option {
	return func(l *Limiter) {
		l.trier = t
	}
}

func New(stats stat.Stats, opts ...Option) *Limiter {
	l := &Limiter{
		stats: stats,
	}
	for _, opt := range opts {
		opt(l)
	}
	if l.dropper == nil {
		l.dropper = bbr.NewBBR(stats)
	}
	if l.trier == nil {
		fq, _ := fair_queue.NewFairQueue(stats)
		l.trier = fq
	}
	return l
}

type token struct {
	start time.Time
	ready <-chan struct{}
	stats stat.Stats
	done  func(canceled bool)
}

func (t *token) Ready() <-chan struct{} {
	return t.ready
}

func (t *token) Done(state rate_limiter.State, _ error) {
	rt := window.Now().UnixNano() - t.start.UnixNano()
	t.stats.RTStat().Add(rt)
	t.stats.AddInFlight(-1)
	canceled := false
	if state == rate_limiter.StateTimeout {
		t.stats.TimeoutStat().Add(1)
		canceled = true
	}
	t.stats.PassStat().Add(1)
	t.done(canceled)
}

func (l *Limiter) Allow(id string) (rate_limiter.Token, error) {
	if l.dropper.Drop() {
		return nil, rate_limiter.ErrLimitExceed()
	}
	ready, done, ok := l.trier.Try(id)
	if !ok {
		return nil, rate_limiter.ErrLimitExceed()
	}
	start := window.Now()
	l.stats.AddInFlight(1)
	return &token{
		start: start,
		ready: ready,
		stats: l.stats,
		done:  done,
	}, nil
}
