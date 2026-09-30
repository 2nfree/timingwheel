package timingwheel

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// TimingWheel 时间轮
type TimingWheel struct {
	tick      int64 // 时间轮的刻度，以毫秒为单位
	wheelSize int64 // 时间轮的槽数

	interval    int64 // 时间轮的总时间跨度，以毫秒为单位
	currentTime int64 // 当前时间，以毫秒为单位
	buckets     []*bucket
	queue       *DelayQueue

	overflowWheel unsafe.Pointer // 更高级的时间轮

	exitC     chan struct{}
	waitGroup sync.WaitGroup
}

// NewTimingWheel 创建并初始化一个新的时间轮实例
func NewTimingWheel(tick time.Duration, wheelSize int64) *TimingWheel {
	tickMs := int64(tick / time.Millisecond)
	if tickMs <= 0 {
		panic(errors.New("tick must be greater than or equal to 1ms"))
	}
	startMs := time.Now().UnixNano() / int64(time.Millisecond)
	return newTimingWheel(
		tickMs,
		wheelSize,
		startMs,
		NewDelayQueue(int(wheelSize)),
	)
}

// 初始化时间轮的各个字段，并创建每个 bucket
func newTimingWheel(tickMs int64, wheelSize int64, startMs int64, queue *DelayQueue) *TimingWheel {
	buckets := make([]*bucket, wheelSize)
	for i := range buckets {
		buckets[i] = newBucket()
	}
	return &TimingWheel{
		tick:        tickMs,
		wheelSize:   wheelSize,
		currentTime: truncate(startMs, tickMs),
		interval:    tickMs * wheelSize,
		buckets:     buckets,
		queue:       queue,
		exitC:       make(chan struct{}),
	}
}

// add 方法将定时器添加到当前时间轮或更高级的时间轮中
func (tw *TimingWheel) add(t *Timer) bool {
	currentTime := atomic.LoadInt64(&tw.currentTime)

	if t.expiration < currentTime+tw.tick { // 如果定时器已经过期，返回 false
		return false
	} else if t.expiration < currentTime+tw.interval { // 如果定时器的到期时间在当前时间轮的时间范围内，将其添加到相应的 bucket
		virtualID := t.expiration / tw.tick
		b := tw.buckets[virtualID%tw.wheelSize]
		b.Add(t)
		if b.SetExpiration(virtualID * tw.tick) {
			tw.queue.Offer(b, b.Expiration())
		}

		return true
	} else { // 如果定时器的到期时间超出了当前时间轮的时间范围，将其添加到更高级的时间轮中
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,
					tw.wheelSize,
					currentTime,
					tw.queue,
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}
		return (*TimingWheel)(overflowWheel).add(t)
	}
}

// 尝试将定时器添加到时间轮中，如果定时器已经过期，直接执行定时任务
func (tw *TimingWheel) addOrRun(t *Timer) {
	if !tw.add(t) {
		go t.task()
	}
}

// 推进时间轮的时间，并尝试推进更高级时间轮的时间
func (tw *TimingWheel) advanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if expiration >= currentTime+tw.tick {
		currentTime = truncate(expiration, tw.tick)
		atomic.StoreInt64(&tw.currentTime, currentTime)
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimingWheel)(overflowWheel).advanceClock(currentTime)
		}
	}
}

// 启动时间轮
func (tw *TimingWheel) Start() {
	// 轮询延迟队列
	tw.waitGroup.Add(1)
	go func() {
		defer tw.waitGroup.Done()
		tw.queue.Poll(tw.exitC, func() int64 {
			return time.Now().UnixNano() / int64(time.Millisecond)
		})
	}()
	// 处理到期的桶
	tw.waitGroup.Add(1)
	go func() {
		defer tw.waitGroup.Done()
		for {
			select {
			case elem := <-tw.queue.C:
				b := elem.(*bucket)
				tw.advanceClock(b.Expiration())
				b.Flush(tw.addOrRun)
			case <-tw.exitC:
				return
			}
		}
	}()
}

// 停止时间轮，关闭 exitC 通道并等待所有 goroutine 退出
func (tw *TimingWheel) Stop() {
	close(tw.exitC)
	tw.waitGroup.Wait()
}

// 等待指定的持续时间后调用函数 f，并返回一个定时器
func (tw *TimingWheel) AfterFunc(d time.Duration, f func()) *Timer {
	t := &Timer{
		expiration: time.Now().Add(d).UnixNano() / int64(time.Millisecond),
		task:       f,
	}
	tw.addOrRun(t)
	return t
}

// Scheduler 定时调度器接口
type Scheduler interface {
	Next(time.Time) time.Time
}

// 根据调度器 s 的计划调用函数 f，并返回一个定时器
func (tw *TimingWheel) ScheduleFunc(s Scheduler, f func()) (t *Timer) {
	expiration := s.Next(time.Now())
	if expiration.IsZero() {
		return
	}
	t = &Timer{
		expiration: expiration.UnixNano() / int64(time.Millisecond),
		task: func() {
			expiration := s.Next(time.Unix(0, t.expiration*int64(time.Millisecond)))
			if !expiration.IsZero() {
				t.expiration = expiration.UnixNano() / int64(time.Millisecond)
				tw.addOrRun(t)
			}
			f()
		},
	}
	tw.addOrRun(t)
	return
}

// 返回将 x 向零方向舍入为 m 的倍数的结果, 如果 m <= 0，则返回未改变的 x
func truncate(x, m int64) int64 {
	if m <= 0 {
		return x
	}
	return x - x%m
}
