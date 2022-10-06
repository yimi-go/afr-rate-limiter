package bbr

import (
	"sync/atomic"
	"time"

	"github.com/yimi-go/rate-limiter"

	"github.com/yimi-go/afr-rate-limiter/stat"
)

// counterCache is used to cache maxPASS and minRt result.
// Value of current bucket is not counted in real time.
// Cache time is equal to a bucket duration.
type counterCache struct {
	val      int64
	position int64
}

// options of bbr limiter.
type options struct {
	// TimeoutThreshold defines how many timeout requests may be ignored within
	// the window before the limiter taking action to deny new requests.
	//
	// Either threshold of TimeoutThreshold or MeanRTThreshold will trigger
	// the limiter to work.
	TimeoutThreshold int64
	// MeanRTThreshold defines how much mean(EMA) request time is allowed
	// within the window before the limiter taking action to deny new requests.
	//
	// A non-positive value means this threshold would not take effect.
	//
	// Either threshold of TimeoutThreshold or MeanRTThreshold will trigger
	// the limiter to work.
	MeanRTThreshold time.Duration
}

type Option func(o *options)

// WithTimeoutThreshold produces an Option that set TimeoutThreshold.
//
// TimeoutThreshold defines how many timeout requests may be ignored within
// the window before the limiter taking action to deny new requests.
//
// Either threshold of TimeoutThreshold or MeanRTThreshold will trigger
// the limiter to work.
func WithTimeoutThreshold(threshold int64) Option {
	return func(o *options) {
		o.TimeoutThreshold = threshold
	}
}

// WithMeanRTThreshold produces an Option that set MeanRTThreshold.
//
// MeanRTThreshold defines how much mean(EMA) request time is allowed
// within the window before the limiter taking action to deny new requests.
//
// A non-positive value means this threshold would not take effect.
//
// Either threshold of TimeoutThreshold or MeanRTThreshold will trigger
// the limiter to work.
func WithMeanRTThreshold(threshold time.Duration) Option {
	return func(o *options) {
		o.MeanRTThreshold = threshold
	}
}

// BBR implements bbr-like limiter.
type BBR struct {
	prevDropTime   atomic.Value
	maxPASSCache   atomic.Value
	minRtCache     atomic.Value
	stats          stat.Stats
	opts           options
	bucketDuration time.Duration
}

// NewBBR returns a bbr limiter
func NewBBR(stats stat.Stats, opts ...Option) *BBR {
	opt := options{
		TimeoutThreshold: 200,
		MeanRTThreshold:  time.Millisecond * 300,
	}
	for _, o := range opts {
		o(&opt)
	}

	limiter := &BBR{
		opts:           opt,
		stats:          stats,
		bucketDuration: stats.TimeoutStat().BucketDuration(),
	}

	return limiter
}

func (bbr *BBR) maxPASS() int64 {
	passCache := bbr.maxPASSCache.Load()
	position := bbr.stats.PassStat().Position()
	if passCache != nil {
		cc := passCache.(*counterCache)
		if position <= cc.position {
			return cc.val
		}
	}
	rawMaxPass := bbr.stats.MaxBucketPass()
	bbr.maxPASSCache.Store(&counterCache{
		val:      rawMaxPass,
		position: position,
	})
	return rawMaxPass
}

// 注意返回的是 nano second
func (bbr *BBR) minRT() int64 {
	rtCache := bbr.minRtCache.Load()
	position := bbr.stats.RTStat().Position()
	if rtCache != nil {
		cc := rtCache.(*counterCache)
		if position <= cc.position {
			return cc.val
		}
	}
	rawMinRT := bbr.stats.MinBucketRT()
	if rawMinRT <= 0 {
		rawMinRT = 1
	}
	bbr.minRtCache.Store(&counterCache{
		val:      rawMinRT,
		position: position,
	})
	return rawMinRT
}

func (bbr *BBR) maxInFlight() int64 {
	// maxPass （次/桶） / bucketDuration （ns/桶） * Second （ns/s） 每秒最大QPS （次/s）
	// minRT （ns） / second (ns/s) 最小耗时（s）
	// 相乘得 最大 inflight 请求
	// 化简 maxPass * minRT / bucketDuration
	// (maxPass * minRT + bucketDuration - 1) / bucketDuration
	return (bbr.maxPASS()*bbr.minRT() + int64(bbr.bucketDuration) - 1) / int64(bbr.bucketDuration)
}

var Now = time.Now

func (bbr *BBR) Drop() bool {
	now := Now().UnixNano()
	if bbr.stats.TimeoutStat().Aggregation(0).Count() < bbr.opts.TimeoutThreshold /*超时数不超过阈值*/ &&
		bbr.stats.RTEma() < bbr.opts.MeanRTThreshold /*加权平均响应时间不超过阈值*/ {
		// current timeout number below the threshold
		prevDropTime, _ := bbr.prevDropTime.Load().(int64)
		if prevDropTime == 0 {
			// haven't start drop,
			// accept current request
			return false
		}
		if now-prevDropTime <= int64(time.Second) {
			// just start drop one second ago,
			// check current inflight count
			inFlight := bbr.stats.InFlight()
			return inFlight > 1 && inFlight > bbr.maxInFlight()
		}
		bbr.prevDropTime.Store(int64(0))
		return false
	}
	// current cpu payload exceeds the threshold
	inFlight := bbr.stats.InFlight()
	drop := inFlight > 1 && inFlight > bbr.maxInFlight()
	if drop {
		prevDrop, _ := bbr.prevDropTime.Load().(int64)
		if prevDrop != 0 {
			// already started drop, return directly
			return drop
		}
		// store start drop time
		bbr.prevDropTime.Store(now)
	}
	return drop
}

type token struct {
	start time.Time
	stats stat.Stats
}

func (t *token) Ready() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (t *token) Done(state rate_limiter.State, _ error) {
	rt := Now().UnixNano() - t.start.UnixNano()
	t.stats.RTStat().Add(rt)
	t.stats.AddInFlight(-1)
	if state == rate_limiter.StateTimeout {
		t.stats.TimeoutStat().Add(1)
	}
	t.stats.PassStat().Add(1)
}

func (bbr *BBR) Allow(_ string) (rate_limiter.Token, error) {
	if bbr.Drop() {
		return nil, rate_limiter.ErrLimitExceed()
	}
	start := Now()
	bbr.stats.AddInFlight(1)
	return &token{
		start: start,
		stats: bbr.stats,
	}, nil
}
