package bbr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yimi-go/window"

	"github.com/yimi-go/afr-rate-limiter/stat"
	limiter "github.com/yimi-go/rate-limiter"
)

func TestWithTimeoutThreshold(t *testing.T) {
	o := &options{}
	WithTimeoutThreshold(500)(o)
	assert.Equal(t, int64(500), o.TimeoutThreshold)
}

func TestWithMeanRTThreshold(t *testing.T) {
	o := &options{}
	WithMeanRTThreshold(time.Second)(o)
	assert.Equal(t, time.Second, o.MeanRTThreshold)
}

func TestNewBBR(t *testing.T) {
	s := stat.New()
	bbr := NewBBR(s,
		WithMeanRTThreshold(time.Second),
		WithTimeoutThreshold(500),
		WithMeanRTThreshold(time.Second))
	assert.Equal(t, int64(500), bbr.opts.TimeoutThreshold)
	assert.Equal(t, time.Second, bbr.opts.MeanRTThreshold)
	assert.Same(t, s, bbr.stats)
	assert.Equal(t, time.Duration(1<<30), bbr.bucketDuration)
}

func TestBBR_maxPass(t *testing.T) {
	now := time.Now()
	window.Now = func() time.Time { return now }
	defer func() { window.Now = time.Now }()
	s := stat.New()
	bbr := NewBBR(s)
	assert.Equal(t, int64(1), bbr.maxPASS())
	assert.Equal(t, int64(1), bbr.maxPASS())
	now = now.Add(s.PassStat().BucketDuration())
	assert.Equal(t, int64(1), bbr.maxPASS())
	s.PassStat().Add(100)
	assert.Equal(t, int64(1), bbr.maxPASS())
	now = now.Add(s.PassStat().BucketDuration())
	assert.Equal(t, int64(100), bbr.maxPASS())
	assert.Equal(t, int64(100), bbr.maxPASS())
}

func TestBBR_minRT(t *testing.T) {
	now := time.Now()
	window.Now = func() time.Time { return now }
	defer func() { window.Now = time.Now }()
	s := stat.New()
	bbr := NewBBR(s)
	assert.Equal(t, int64(1), bbr.minRT())
	assert.Equal(t, int64(1), bbr.minRT())
	now = now.Add(s.PassStat().BucketDuration())
	assert.Equal(t, int64(1), bbr.minRT())
	s.RTStat().Add(100)
	assert.Equal(t, int64(1), bbr.minRT())
	now = now.Add(s.PassStat().BucketDuration())
	assert.Equal(t, int64(100), bbr.minRT())
	assert.Equal(t, int64(100), bbr.minRT())
}

func TestBBR_maxInFlight(t *testing.T) {
	now := time.Now()
	window.Now = func() time.Time { return now }
	defer func() { window.Now = time.Now }()
	s := stat.New()
	bbr := NewBBR(s)
	assert.Equal(t, int64(1), bbr.maxInFlight())
	now = now.Add(s.PassStat().BucketDuration())
	s.PassStat().Add(1)
	s.RTStat().Add(int64(s.PassStat().BucketDuration()) + 2)
	now = now.Add(s.PassStat().BucketDuration() * 2)
	assert.Equal(t, int64(2), bbr.maxInFlight())
}

func TestBBR_Drop(t *testing.T) {
	now := Now()
	Now = func() time.Time { return now }
	defer func() { Now = time.Now }()
	tests := []struct {
		bbr  *BBR
		name string
		want bool
	}{
		{
			name: "not_start_drop",
			bbr: func() *BBR {
				b := &BBR{
					stats: &testStats{
						timeout: &testWin{
							agg: func(u uint) window.Aggregator {
								return &testAgg{
									count: 33,
								}
							},
						},
						rtEma: 100 * time.Millisecond,
					},
					opts: options{
						TimeoutThreshold: 200,
						MeanRTThreshold:  time.Millisecond * 500,
					},
					bucketDuration: time.Duration(1 << 30),
				}
				return b
			}(),
			want: false,
		},
		{
			name: "just_start_drop_no_inflight",
			bbr: func() *BBR {
				b := &BBR{
					stats: &testStats{
						timeout: &testWin{
							agg: func(u uint) window.Aggregator {
								return &testAgg{
									count: 33,
								}
							},
						},
						rtEma:    100 * time.Millisecond,
						inflight: 0,
					},
					opts: options{
						TimeoutThreshold: 200,
						MeanRTThreshold:  time.Millisecond * 500,
					},
					bucketDuration: time.Duration(1 << 30),
				}
				b.prevDropTime.Store(now.UnixNano() - 1)
				return b
			}(),
			want: false,
		},
		{
			name: "just_start_drop_inflight_blow_max",
			bbr: func() *BBR {
				b := &BBR{
					stats: &testStats{
						timeout: &testWin{
							agg: func(u uint) window.Aggregator {
								return &testAgg{
									count: 33,
								}
							},
						},
						rtEma:    100 * time.Millisecond,
						inflight: 20,
						maxPass:  1000000,
						minRT:    5000,
						pass:     &testWin{},
						rt:       &testWin{},
					},
					opts: options{
						TimeoutThreshold: 200,
						MeanRTThreshold:  time.Millisecond * 500,
					},
					bucketDuration: time.Duration(1 << 20),
				}
				b.prevDropTime.Store(now.UnixNano() - 1)
				return b
			}(),
			want: false,
		},
		{
			name: "just_start_drop_inflight_overflow",
			bbr: func() *BBR {
				b := &BBR{
					stats: &testStats{
						timeout: &testWin{
							agg: func(u uint) window.Aggregator {
								return &testAgg{
									count: 33,
								}
							},
						},
						rtEma:    100 * time.Millisecond,
						inflight: 5000,
						maxPass:  1000000,
						minRT:    5000,
						pass:     &testWin{},
						rt:       &testWin{},
					},
					opts: options{
						TimeoutThreshold: 200,
						MeanRTThreshold:  time.Millisecond * 500,
					},
					bucketDuration: time.Duration(1 << 20),
				}
				b.prevDropTime.Store(now.UnixNano() - 1)
				return b
			}(),
			want: true,
		},
		{
			name: "start_drop_inflight_second_before",
			bbr: func() *BBR {
				b := &BBR{
					stats: &testStats{
						timeout: &testWin{
							agg: func(u uint) window.Aggregator {
								return &testAgg{
									count: 33,
								}
							},
						},
						rtEma: 100 * time.Millisecond,
					},
					opts: options{
						TimeoutThreshold: 200,
						MeanRTThreshold:  time.Millisecond * 500,
					},
					bucketDuration: time.Duration(1 << 20),
				}
				b.prevDropTime.Store(now.UnixNano() - int64(time.Minute))
				return b
			}(),
			want: false,
		},
		{
			name: "threshold_overflow_but_already_drop",
			bbr: func() *BBR {
				b := &BBR{
					stats: &testStats{
						timeout: &testWin{
							agg: func(u uint) window.Aggregator {
								return &testAgg{
									count: 300,
								}
							},
						},
						inflight: 5000,
						maxPass:  1000000,
						minRT:    5000,
						pass:     &testWin{},
						rt:       &testWin{},
					},
					opts: options{
						TimeoutThreshold: 200,
					},
					bucketDuration: time.Duration(1 << 20),
				}
				b.prevDropTime.Store(now.UnixNano() - int64(time.Minute))
				return b
			}(),
			want: true,
		},
		{
			name: "threshold_overflow_drop",
			bbr: func() *BBR {
				b := &BBR{
					stats: &testStats{
						timeout: &testWin{
							agg: func(u uint) window.Aggregator {
								return &testAgg{
									count: 300,
								}
							},
						},
						inflight: 5000,
						maxPass:  1000000,
						minRT:    5000,
						pass:     &testWin{},
						rt:       &testWin{},
					},
					opts: options{
						TimeoutThreshold: 200,
					},
					bucketDuration: time.Duration(1 << 20),
				}
				return b
			}(),
			want: true,
		},
		{
			name: "threshold_overflow_not_drop",
			bbr: func() *BBR {
				b := &BBR{
					stats: &testStats{
						timeout: &testWin{
							agg: func(u uint) window.Aggregator {
								return &testAgg{
									count: 300,
								}
							},
						},
						inflight: 4000,
						maxPass:  1000000,
						minRT:    5000,
						pass:     &testWin{},
						rt:       &testWin{},
					},
					opts: options{
						TimeoutThreshold: 200,
					},
					bucketDuration: time.Duration(1 << 20),
				}
				return b
			}(),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.bbr.Drop()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBBR_Allow(t *testing.T) {
	now := Now()
	Now = func() time.Time { return now }
	window.Now = func() time.Time { return now }
	defer func() {
		Now = time.Now
		window.Now = time.Now
	}()
	stats := stat.New()
	bbr := NewBBR(stats)
	bbr.prevDropTime.Store(Now().UnixNano() - 1)
	bbr.stats.AddInFlight(2)

	allow, err := bbr.Allow("")
	assert.True(t, limiter.IsErrLimitExceed(err))
	assert.Nil(t, allow)
	assert.Equal(t, int64(2), bbr.stats.InFlight())
	bbr.prevDropTime.Store(int64(0))
	bbr.stats.AddInFlight(-2)

	allow, err = bbr.Allow("")
	assert.Nil(t, err)
	assert.NotNil(t, allow)
	select {
	case <-allow.Ready():
	default:
		assert.Fail(t, "should ready")
	}
	assert.Equal(t, int64(1), bbr.stats.InFlight())
	now = now.Add(time.Second)
	allow.Done(limiter.StateOk, nil)
	assert.Equal(t, int64(0), bbr.stats.InFlight())
	assert.Equal(t, int64(1), bbr.stats.RTStat().Aggregation(0).Count())
	assert.Equal(t, int64(time.Second), bbr.stats.RTStat().Aggregation(0).Sum())
	assert.Equal(t, int64(1), bbr.stats.PassStat().Aggregation(0).Count())
	assert.Equal(t, int64(1), bbr.stats.PassStat().Aggregation(0).Sum())
	assert.Equal(t, int64(0), bbr.stats.TimeoutStat().Aggregation(0).Count())
	assert.Equal(t, int64(0), bbr.stats.TimeoutStat().Aggregation(0).Sum())

	allow, err = bbr.Allow("")
	assert.Nil(t, err)
	assert.NotNil(t, allow)
	select {
	case <-allow.Ready():
	default:
		assert.Fail(t, "should ready")
	}
	assert.Equal(t, int64(1), bbr.stats.InFlight())
	now = now.Add(time.Second)
	allow.Done(limiter.StateTimeout, nil)
	assert.Equal(t, int64(0), bbr.stats.InFlight())
	assert.Equal(t, int64(2), bbr.stats.RTStat().Aggregation(0).Count())
	assert.Equal(t, int64(time.Second*2), bbr.stats.RTStat().Aggregation(0).Sum())
	assert.Equal(t, int64(2), bbr.stats.PassStat().Aggregation(0).Count())
	assert.Equal(t, int64(2), bbr.stats.PassStat().Aggregation(0).Sum())
	assert.Equal(t, int64(1), bbr.stats.TimeoutStat().Aggregation(0).Count())
	assert.Equal(t, int64(1), bbr.stats.TimeoutStat().Aggregation(0).Sum())
}

type testAgg struct {
	reduce func(func(bucket window.Bucket) bool)
	min    int64
	max    int64
	sum    int64
	count  int64
	avg    float64
}

func (t *testAgg) Min() int64                                      { return t.min }
func (t *testAgg) Max() int64                                      { return t.max }
func (t *testAgg) Avg() float64                                    { return t.avg }
func (t *testAgg) Sum() int64                                      { return t.sum }
func (t *testAgg) Count() int64                                    { return t.count }
func (t *testAgg) Reduce(f func(bucket window.Bucket) (done bool)) { t.reduce(f) }

type testWin struct {
	append    func(int64)
	add       func(int64)
	agg       func(uint) window.Aggregator
	position  int64
	bucketNum int64
	bd        time.Duration
}

func (t *testWin) Position() int64                               { return t.position }
func (t *testWin) Append(val int64)                              { t.append(val) }
func (t *testWin) Add(val int64)                                 { t.add(val) }
func (t *testWin) Aggregation(skipRecent uint) window.Aggregator { return t.agg(skipRecent) }
func (t *testWin) BucketNum() int64                              { return t.bucketNum }
func (t *testWin) BucketDuration() time.Duration                 { return t.bd }

type testStats struct {
	rt          window.Window
	timeout     window.Window
	pass        window.Window
	addInFlight func(int64)
	maxPass     int64
	minRT       int64
	inflight    int64
	rtEma       time.Duration
	anyTimeout  bool
}

func (t *testStats) RTStat() window.Window      { return t.rt }
func (t *testStats) TimeoutStat() window.Window { return t.timeout }
func (t *testStats) PassStat() window.Window    { return t.pass }
func (t *testStats) MaxBucketPass() int64       { return t.maxPass }
func (t *testStats) MinBucketRT() int64         { return t.minRT }
func (t *testStats) RTEma() time.Duration       { return t.rtEma }
func (t *testStats) AddInFlight(delta int64)    { t.addInFlight(delta) }
func (t *testStats) InFlight() int64            { return t.inflight }
func (t *testStats) AnyTimeout() bool           { return t.anyTimeout }
