package fair_queue

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yimi-go/afr-rate-limiter/shufflesharding"
	"github.com/yimi-go/afr-rate-limiter/stat"
	limiter "github.com/yimi-go/rate-limiter"
)

var (
	closedChan = make(chan struct{})
)

func init() {
	close(closedChan)
}

type options struct {
	queueNum int
	queueCap int
	handSize int
	initSeat int64
}

type Option func(o *options)

func WithQueueNum(num int) Option {
	return func(o *options) {
		o.queueNum = num
	}
}

func WithQueueCap(cap int) Option {
	return func(o *options) {
		o.queueCap = cap
	}
}

func WithHandSize(hand int) Option {
	return func(o *options) {
		o.handSize = hand
	}
}

func WithInitSeat(seat int64) Option {
	return func(o *options) {
		o.initSeat = seat
	}
}

type dealer interface {
	DealIntoHand(hashValue uint64, hand []int) []int
}

type queueSetInterface interface {
	switchAndMinHeapify(q *queue)
	popFirst() *queueNode
	get(index int) *queue
}

type FairQueue struct {
	stats  stat.Stats
	set    queueSetInterface
	dealer dealer

	maxSeat  int64
	busySeat int64
	enqueues int64
}

func NewFairQueue(stats stat.Stats, opts ...Option) (*FairQueue, error) {
	// 默认 128 队列，8手
	// 发生踩踏概率表：https://kubernetes.io/docs/concepts/cluster-administration/flow-control/#prioritylevelconfiguration
	o := &options{
		queueNum: 128,
		handSize: 8,
		queueCap: 32,
		initSeat: 3000,
	}
	for _, opt := range opts {
		opt(o)
	}
	dealer, err := shufflesharding.NewDealer(o.queueNum, o.handSize)
	if err != nil {
		return nil, err
	}
	return &FairQueue{
		stats:   stats,
		set:     newQueueSet(o.queueNum, o.queueCap),
		dealer:  dealer,
		maxSeat: o.initSeat,
	}, nil
}

func (fq *FairQueue) Try(id string) (ready <-chan struct{}, done func(canceled bool), allowed bool) {
	doneOnce := sync.Once{}
	if atomic.LoadInt64(&fq.busySeat) < atomic.LoadInt64(&fq.maxSeat) {
		// 当前有空闲处理席位。直接处理
		atomic.AddInt64(&fq.busySeat, 1)
		return closedChan, func(_ bool) {
			doneOnce.Do(func() { fq.onReqDone() })
		}, true
	}
	// 执行分片混洗，选择最短队列
	q := fq.choose(id)
	if q != nil {
		// 找到，尝试入队
		if qn, heapify := q.tryEnqueue(); qn != nil {
			qn.queueSetPosition = atomic.AddInt64(&fq.enqueues, 1)
			if heapify {
				fq.set.switchAndMinHeapify(q)
			}
			// 成功入队
			return qn.ready, func(canceled bool) {
				doneOnce.Do(func() {
					if canceled {
						q.dequeue(qn)
					}
					fq.onReqDone()
				})
			}, true
		}
	}
	// 没有找到 或 没能入队。检查是否可以扩席位
	if fq.stats.AnyTimeout() {
		// 当前统计窗口有超时，谨慎地拒绝请求，不增加席位
		return nil, nil, false
	}
	// 临时增加席位执行。
	atomic.AddInt64(&fq.busySeat, 1)
	// 直接处理，不入队
	return closedChan, func(canceled bool) {
		doneOnce.Do(func() {
			if !canceled && !fq.stats.AnyTimeout() {
				// 增开后也没有超时，增开有效
				atomic.AddInt64(&fq.maxSeat, 1)
				fq.onReqDone()
			} else {
				atomic.AddInt64(&fq.busySeat, -1)
			}
		})
	}, true
}

func (fq *FairQueue) onReqDone() {
	// 取下一个等待最久请求
	qn := fq.set.popFirst()
	if qn == nil {
		// 没有排队请求。释放席位
		atomic.AddInt64(&fq.busySeat, -1)
		return
	}
	// 通知处理请求。被通知请求继承席位
	close(qn.ready)
}

func (fq *FairQueue) choose(id string) *queue {
	hand := fq.dealer.DealIntoHand(hashValue(id), make([]int, 15))
	handSize := len(hand)
	offset := int(atomic.LoadInt64(&fq.enqueues) % int64(handSize))
	var bestQueue *queue
	minQueueLength := math.MaxInt
	for i := 0; i < handSize; i++ {
		queueIdx := hand[(offset+i)%handSize]
		q := fq.set.get(queueIdx)
		queueLength := q.length()
		if queueLength < minQueueLength {
			minQueueLength, bestQueue = queueLength, q
		}
	}
	return bestQueue
}

func hashValue(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.LittleEndian.Uint64(sum[:8])
}

var Now = time.Now

type token struct {
	start time.Time
	stats stat.Stats
	ready <-chan struct{}
	done  func(canceled bool)
}

func (t *token) Ready() <-chan struct{} {
	return t.ready
}

func (t *token) Done(state limiter.State, _ error) {
	rt := Now().UnixNano() - t.start.UnixNano()
	t.stats.RTStat().Add(rt)
	t.stats.AddInFlight(-1)
	if state == limiter.StateTimeout {
		t.stats.TimeoutStat().Add(1)
	}
	t.stats.PassStat().Add(1)
	t.done(state == limiter.StateTimeout)
}

func (fq *FairQueue) Allow(id string) (limiter.Token, error) {
	ready, done, allowed := fq.Try(id)
	if !allowed {
		return nil, limiter.ErrLimitExceed()
	}
	start := Now()
	fq.stats.AddInFlight(1)
	return &token{
		start: start,
		stats: fq.stats,
		ready: ready,
		done:  done,
	}, nil
}
