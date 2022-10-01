package stat

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/yimi-go/window"
)

// Stats 统计信息
type Stats interface {
	// RTStat 响应时间统计
	RTStat() window.Window
	// TimeoutStat 超时请求数统计
	TimeoutStat() window.Window
	// PassStat 完成请求数统计
	PassStat() window.Window
	// MaxBucketPass 返回最大单 Bucket 未超时完成请求数
	MaxBucketPass() int64
	// MinBucketRT 返回最小单 Bucket 平均响应时间
	MinBucketRT() int64
	// RTEma 返回统计的加权平均响应时间
	RTEma() time.Duration
	// AddInFlight 增加或减少（传负值）InFlight 请求计数
	AddInFlight(delta int64)
	// InFlight 当前 InFlight 请求数
	InFlight() int64
	// AnyTimeout 当前统计窗口内是否有响应超时
	AnyTimeout() bool
}

type stats struct {
	passStat    window.Window
	timeoutStat window.Window
	rtStat      window.Window

	decay    float64
	rtEma    int64
	inFlight int64
}

type options struct {
	bucketNum             int
	requireBucketDuration time.Duration
	rtEmaDecay            float64
}

type Option func(s *options)

func WithBucketNum(b int) Option {
	return func(s *options) {
		s.bucketNum = b
	}
}

func WithRequireBucketDuration(d time.Duration) Option {
	return func(s *options) {
		s.requireBucketDuration = d
	}
}

func WithRTEmaDecay(d float64) Option {
	return func(s *options) {
		s.rtEmaDecay = d
	}
}

func New(opts ...Option) Stats {
	wo := &options{
		bucketNum:             20,                     /* about 21.5s: 21.47483648s */
		requireBucketDuration: time.Duration(1 << 30), /* about 1s: 1.073741824s */
		rtEmaDecay:            0.8,                    /* 越低，对响应时间的变化越敏感 */
	}
	for _, opt := range opts {
		opt(wo)
	}
	s := &stats{
		passStat:    window.NewWindow(wo.bucketNum, wo.requireBucketDuration),
		timeoutStat: window.NewWindow(wo.bucketNum, wo.requireBucketDuration),
		rtStat:      window.NewWindow(wo.bucketNum, wo.requireBucketDuration),
		decay:       wo.rtEmaDecay,
	}
	go s.runUpdateRTEma()
	return s
}

func (s *stats) runUpdateRTEma() {
	bd := s.rtStat.BucketDuration()
	ticker := time.NewTicker(bd) // same to rt stat bucket duration
	defer func() {
		ticker.Stop()
		if err := recover(); err != nil {
			go s.runUpdateRTEma()
		}
	}()
	for range ticker.C {
		s.updateRTEma()
	}
}

func (s *stats) updateRTEma() {
	curRT := int64(0)
	s.rtStat.Aggregation(1).Reduce(func(bucket window.Bucket) (done bool) {
		if bucket.Count() == 0 {
			return
		}
		var sum int64
		for _, v := range bucket.Data() {
			sum += v
		}
		curRT = int64(float64(curRT)*s.decay + float64(sum)*(1-s.decay))
		return
	})
	if curRT == 0 {
		curRT = 1
	}
	atomic.StoreInt64(&s.rtEma, curRT)
}

func (s *stats) RTStat() window.Window {
	return s.rtStat
}

func (s *stats) TimeoutStat() window.Window {
	return s.timeoutStat
}

func (s *stats) PassStat() window.Window {
	return s.passStat
}

func (s *stats) MaxBucketPass() int64 {
	var result int64 = 1
	s.passStat.Aggregation(1).Reduce(func(bucket window.Bucket) (done bool) {
		var count int64
		for _, v := range bucket.Data() {
			count += v
		}
		if count > result {
			result = count
		}
		return
	})
	return result
}

func (s *stats) MinBucketRT() int64 {
	var result int64 = math.MaxInt64
	s.rtStat.Aggregation(1).Reduce(func(bucket window.Bucket) (done bool) {
		if len(bucket.Data()) == 0 {
			return
		}
		var total int64
		for _, v := range bucket.Data() {
			total += v
		}
		avg := (total + bucket.Count() - 1) / bucket.Count()
		if avg < result {
			result = avg
		}
		return
	})
	if result == math.MaxInt64 {
		return 0
	}
	return result
}

func (s *stats) RTEma() time.Duration {
	return time.Duration(atomic.LoadInt64(&s.rtEma))
}

func (s *stats) AddInFlight(num int64) {
	atomic.AddInt64(&s.inFlight, num)
}

func (s *stats) InFlight() int64 {
	return atomic.LoadInt64(&s.inFlight)
}

func (s *stats) AnyTimeout() bool {
	var result bool
	s.timeoutStat.Aggregation(0).Reduce(func(bucket window.Bucket) bool {
		result = bucket.Count() > 0
		return result
	})
	return result
}
