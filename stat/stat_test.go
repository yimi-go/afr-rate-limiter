package stat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yimi-go/window"
)

func TestWithBucketNum(t *testing.T) {
	o := &options{}
	WithBucketNum(99)(o)
	assert.Equal(t, 99, o.bucketNum)
}

func TestWithRequireBucketDuration(t *testing.T) {
	o := &options{}
	WithRequireBucketDuration(time.Second)(o)
	assert.Equal(t, time.Second, o.requireBucketDuration)
}

func TestWithRTEmaDecay(t *testing.T) {
	o := &options{}
	WithRTEmaDecay(.5)(o)
	assert.Equal(t, .5, o.rtEmaDecay)
}

func TestNew(t *testing.T) {
	s := New(WithRTEmaDecay(.5)).(*stats)
	assert.Equal(t, int64(20), s.passStat.BucketNum())
	assert.Equal(t, time.Duration(1<<30), s.passStat.BucketDuration())
	assert.Equal(t, int64(20), s.timeoutStat.BucketNum())
	assert.Equal(t, time.Duration(1<<30), s.timeoutStat.BucketDuration())
	assert.Equal(t, int64(20), s.rtStat.BucketNum())
	assert.Equal(t, time.Duration(1<<30), s.rtStat.BucketDuration())
	assert.Equal(t, .5, s.decay)
	assert.Equal(t, int64(0), s.rtEma)
	assert.Equal(t, int64(0), s.inFlight)
}

func Test_stats_updateRTEma(t *testing.T) {
	now := time.Now()
	window.Now = func() time.Time { return now }
	defer func() { window.Now = time.Now }()
	s := New(WithRTEmaDecay(.5)).(*stats)
	s.updateRTEma()
	assert.Equal(t, int64(1), s.rtEma)
	for i := 0; i < 2; i++ {
		s.rtStat.Add(100)
		now = now.Add(s.rtStat.BucketDuration())
	}
	s.updateRTEma()
	assert.Equal(t, int64(75), s.rtEma)
}

func Test_stats_RTStat(t *testing.T) {
	s := New(WithRTEmaDecay(.5)).(*stats)
	assert.Same(t, s.rtStat, s.RTStat())
}

func Test_stats_TimeoutStat(t *testing.T) {
	s := New().(*stats)
	assert.Same(t, s.timeoutStat, s.TimeoutStat())
}

func Test_stats_PassStat(t *testing.T) {
	s := New().(*stats)
	assert.Same(t, s.passStat, s.PassStat())
}

func Test_stats_MaxBucketPass(t *testing.T) {
	now := time.Now()
	window.Now = func() time.Time { return now }
	defer func() { window.Now = time.Now }()
	s := New().(*stats)
	assert.Equal(t, int64(1), s.MaxBucketPass())
	for i := int64(0); i < s.passStat.BucketNum(); i++ {
		s.passStat.Add(i)
		now = now.Add(s.passStat.BucketDuration())
	}
	assert.Equal(t, s.passStat.BucketNum()-1, s.MaxBucketPass())
}

func Test_stats_MinBucketRT(t *testing.T) {
	now := time.Now()
	window.Now = func() time.Time { return now }
	defer func() { window.Now = time.Now }()
	s := New().(*stats)
	assert.Equal(t, int64(0), s.MinBucketRT())
	for i := int64(0); i < s.passStat.BucketNum(); i++ {
		if i%int64(2) == 1 {
			s.rtStat.Add(i)
		}
		now = now.Add(s.rtStat.BucketDuration())
	}
	assert.Equal(t, int64(1), s.MinBucketRT())
}

func Test_stats_RTEma(t *testing.T) {
	s := New().(*stats)
	assert.Equal(t, time.Duration(0), s.RTEma())
	s.updateRTEma()
	assert.Equal(t, time.Duration(1), s.RTEma())
}

func Test_stats_InFlight(t *testing.T) {
	s := New().(*stats)
	assert.Equal(t, int64(0), s.InFlight())
	s.AddInFlight(100)
	assert.Equal(t, int64(100), s.InFlight())
	s.AddInFlight(-1)
	assert.Equal(t, int64(99), s.InFlight())
}

func Test_stats_AnyTimeout(t *testing.T) {
	s := New().(*stats)
	assert.False(t, s.AnyTimeout())
	s.TimeoutStat().Add(1)
	assert.True(t, s.AnyTimeout())
}
