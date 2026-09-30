package timingwheel

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 测试辅助工具

// ExecutionTracker 用于跟踪定时器的执行情况
type ExecutionTracker struct {
	mu         sync.Mutex
	executions []time.Time
	expected   int64
	actual     int64
}

// NewExecutionTracker 创建新的执行追踪器
func NewExecutionTracker() *ExecutionTracker {
	return &ExecutionTracker{
		executions: make([]time.Time, 0),
	}
}

// Track 记录一次执行
func (et *ExecutionTracker) Track() {
	et.mu.Lock()
	defer et.mu.Unlock()
	et.executions = append(et.executions, time.Now())
	atomic.AddInt64(&et.actual, 1)
}

// GetExecutions 获取所有执行时间
func (et *ExecutionTracker) GetExecutions() []time.Time {
	et.mu.Lock()
	defer et.mu.Unlock()
	result := make([]time.Time, len(et.executions))
	copy(result, et.executions)
	return result
}

// GetCount 获取执行次数
func (et *ExecutionTracker) GetCount() int64 {
	return atomic.LoadInt64(&et.actual)
}

// SetExpected 设置期望执行次数
func (et *ExecutionTracker) SetExpected(count int64) {
	atomic.StoreInt64(&et.expected, count)
}

// IsCompleted 检查是否达到期望执行次数
func (et *ExecutionTracker) IsCompleted() bool {
	return atomic.LoadInt64(&et.actual) >= atomic.LoadInt64(&et.expected)
}

// WaitForCompletion 等待达到期望执行次数
func (et *ExecutionTracker) WaitForCompletion(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if et.IsCompleted() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestHelper 测试辅助工具
type TestHelper struct {
	t      *testing.T
	timers []*Timer
	mu     sync.Mutex
}

// NewTestHelper 创建测试辅助工具
func NewTestHelper(t *testing.T) *TestHelper {
	return &TestHelper{
		t:      t,
		timers: make([]*Timer, 0),
	}
}

// AddTimer 添加定时器到清理列表
func (th *TestHelper) AddTimer(timer *Timer) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.timers = append(th.timers, timer)
}

// Cleanup 清理所有定时器
func (th *TestHelper) Cleanup() {
	th.mu.Lock()
	defer th.mu.Unlock()
	for _, timer := range th.timers {
		if timer != nil {
			timer.Stop()
		}
	}
	th.timers = th.timers[:0]
}

// AssertExecutionTime 断言执行时间误差在可接受范围内
func (th *TestHelper) AssertExecutionTime(expected, actual time.Time, tolerance time.Duration, msg string) {
	diff := actual.Sub(expected)
	if diff < 0 {
		diff = -diff
	}
	if diff > tolerance {
		th.t.Errorf("%s: execution time error too large, expected: %v, actual: %v, diff: %v, tolerance: %v",
			msg, expected, actual, diff, tolerance)
	}
}

// AssertExecutionCount 断言执行次数
func (th *TestHelper) AssertExecutionCount(expected, actual int64, msg string) {
	if expected != actual {
		th.t.Errorf("%s: execution count mismatch, expected: %d, actual: %d", msg, expected, actual)
	}
}

// 基础功能测试

// TestNewTimingWheel 测试时间轮的创建
func TestNewTimingWheel(t *testing.T) {
	tests := []struct {
		name      string
		tick      time.Duration
		wheelSize int64
		wantPanic bool
	}{
		{
			name:      "正常参数",
			tick:      100 * time.Millisecond,
			wheelSize: 10,
			wantPanic: false,
		},
		{
			name:      "最小刻度",
			tick:      1 * time.Millisecond,
			wheelSize: 60,
			wantPanic: false,
		},
		{
			name:      "大轮尺寸",
			tick:      1 * time.Second,
			wheelSize: 3600,
			wantPanic: false,
		},
		{
			name:      "零刻度",
			tick:      0,
			wheelSize: 10,
			wantPanic: true,
		},
		{
			name:      "负刻度",
			tick:      -1 * time.Millisecond,
			wheelSize: 10,
			wantPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("NewTimingWheel() 应该 panic，但没有")
					}
				}()
			}

			tw := NewTimingWheel(tt.tick, tt.wheelSize)
			if !tt.wantPanic {
				if tw == nil {
					t.Errorf("NewTimingWheel() 返回了 nil")
					return
				}

				// 验证时间轮的基本属性
				if tw.tick != int64(tt.tick/time.Millisecond) {
					t.Errorf("tick = %v, want %v", tw.tick, int64(tt.tick/time.Millisecond))
				}
				if tw.wheelSize != tt.wheelSize {
					t.Errorf("wheelSize = %v, want %v", tw.wheelSize, tt.wheelSize)
				}
				if tw.interval != tw.tick*tw.wheelSize {
					t.Errorf("interval = %v, want %v", tw.interval, tw.tick*tw.wheelSize)
				}
				if len(tw.buckets) != int(tt.wheelSize) {
					t.Errorf("buckets length = %v, want %v", len(tw.buckets), tt.wheelSize)
				}
				if tw.queue == nil {
					t.Errorf("queue 不应该是 nil")
				}

				// 验证所有 bucket 都已初始化
				for i, bucket := range tw.buckets {
					if bucket == nil {
						t.Errorf("bucket[%d] 是 nil", i)
					}
				}
			}
		})
	}
}

// TestTimingWheelStartStop 测试时间轮的启动和停止
func TestTimingWheelStartStop(t *testing.T) {
	tw := NewTimingWheel(100*time.Millisecond, 10)
	if tw == nil {
		t.Fatal("Failed to create timing wheel")
	}

	// 测试启动
	tw.Start()

	// 等待一段时间确保启动完成
	time.Sleep(50 * time.Millisecond)

	// 测试停止
	tw.Stop()
}

// TestTimingWheelMultipleStartStop 测试多次启动停止
func TestTimingWheelMultipleStartStop(t *testing.T) {
	// 每次循环都创建新的时间轮实例，避免重复关闭channel
	for range 3 {
		tw := NewTimingWheel(100*time.Millisecond, 10)
		if tw == nil {
			t.Fatal("Failed to create timing wheel")
		}

		tw.Start()
		time.Sleep(10 * time.Millisecond)
		tw.Stop()
	}
}

// 定时器功能测试

// TestAfterFunc 测试 AfterFunc 基本功能
func TestAfterFunc(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tests := []struct {
		name     string
		delay    time.Duration
		expected int64
	}{
		{
			name:     "短延迟",
			delay:    100 * time.Millisecond,
			expected: 1,
		},
		{
			name:     "中等延迟",
			delay:    500 * time.Millisecond,
			expected: 1,
		},
		{
			name:     "长延迟",
			delay:    1 * time.Second,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewExecutionTracker()
			tracker.SetExpected(tt.expected)

			startTime := time.Now()
			timer := tw.AfterFunc(tt.delay, func() {
				tracker.Track()
			})

			helper.AddTimer(timer)

			// 等待执行完成
			if !tracker.WaitForCompletion(tt.delay + 200*time.Millisecond) {
				t.Errorf("定时器未在预期时间内执行")
				return
			}

			// 验证执行次数
			helper.AssertExecutionCount(tt.expected, tracker.GetCount(), "定时器执行次数")

			// 验证执行时间
			executions := tracker.GetExecutions()
			if len(executions) > 0 {
				expectedTime := startTime.Add(tt.delay)
				helper.AssertExecutionTime(expectedTime, executions[0], 100*time.Millisecond, "定时器执行时间")
			}
		})
	}
}

// TestAfterFuncMultiple 测试多个 AfterFunc 定时器
func TestAfterFuncMultiple(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tracker := NewExecutionTracker()
	tracker.SetExpected(3)

	delays := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		300 * time.Millisecond,
	}

	// 创建多个定时器
	for i, delay := range delays {
		timer := tw.AfterFunc(delay, func() {
			tracker.Track()
		})
		helper.AddTimer(timer)
		t.Logf("创建定时器 %d，延迟 %v", i, delay)
	}

	// 等待所有定时器执行完成
	if !tracker.WaitForCompletion(500 * time.Millisecond) {
		t.Errorf("部分定时器未在预期时间内执行")
	}

	helper.AssertExecutionCount(3, tracker.GetCount(), "多个定时器执行次数")
}

// TestTimerStop 测试定时器取消功能
func TestTimerStop(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tracker := NewExecutionTracker()

	// 创建一个长延迟的定时器
	timer := tw.AfterFunc(500*time.Millisecond, func() {
		tracker.Track()
	})
	helper.AddTimer(timer)

	// 等待一小段时间后取消定时器
	time.Sleep(100 * time.Millisecond)
	stopped := timer.Stop()

	if !stopped {
		t.Errorf("定时器取消失败")
	}

	// 等待超过原定时器延迟时间
	time.Sleep(600 * time.Millisecond)

	// 验证定时器没有执行
	helper.AssertExecutionCount(0, tracker.GetCount(), "已取消定时器执行次数")
}

// TestTimerStopAfterExecution 测试已执行定时器的取消
func TestTimerStopAfterExecution(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tracker := NewExecutionTracker()
	tracker.SetExpected(1)

	// 创建一个短延迟的定时器
	timer := tw.AfterFunc(100*time.Millisecond, func() {
		tracker.Track()
	})
	helper.AddTimer(timer)

	// 等待定时器执行
	if !tracker.WaitForCompletion(200 * time.Millisecond) {
		t.Errorf("定时器未在预期时间内执行")
	}

	// 尝试取消已执行的定时器
	stopped := timer.Stop()
	if stopped {
		t.Errorf("已执行的定时器不应该能被取消")
	}

	helper.AssertExecutionCount(1, tracker.GetCount(), "已执行定时器次数")
}

// TestTimerStopMultiple 测试多次取消同一定时器
func TestTimerStopMultiple(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tracker := NewExecutionTracker()

	timer := tw.AfterFunc(500*time.Millisecond, func() {
		tracker.Track()
	})
	helper.AddTimer(timer)

	// 第一次取消
	stopped1 := timer.Stop()
	if !stopped1 {
		t.Errorf("第一次取消应该成功")
	}

	// 第二次取消
	stopped2 := timer.Stop()
	if stopped2 {
		t.Errorf("第二次取消应该失败")
	}

	// 验证定时器没有执行
	time.Sleep(600 * time.Millisecond)
	helper.AssertExecutionCount(0, tracker.GetCount(), "多次取消定时器执行次数")
}

// TestExpiredTimer 测试过期定时器立即执行
func TestExpiredTimer(t *testing.T) {
	tw := NewTimingWheel(100*time.Millisecond, 10)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tracker := NewExecutionTracker()
	tracker.SetExpected(1)

	// 创建一个已过期的定时器（负延迟）
	timer := tw.AfterFunc(-100*time.Millisecond, func() {
		tracker.Track()
	})
	helper.AddTimer(timer)

	// 过期定时器应该立即执行
	if !tracker.WaitForCompletion(100 * time.Millisecond) {
		t.Errorf("过期定时器未立即执行")
	}

	helper.AssertExecutionCount(1, tracker.GetCount(), "过期定时器执行次数")
}

// TestZeroDelayTimer 测试零延迟定时器
func TestZeroDelayTimer(t *testing.T) {
	tw := NewTimingWheel(100*time.Millisecond, 10)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tracker := NewExecutionTracker()
	tracker.SetExpected(1)

	// 创建零延迟定时器
	timer := tw.AfterFunc(0, func() {
		tracker.Track()
	})
	helper.AddTimer(timer)

	// 零延迟定时器应该立即执行
	if !tracker.WaitForCompletion(100 * time.Millisecond) {
		t.Errorf("零延迟定时器未立即执行")
	}

	helper.AssertExecutionCount(1, tracker.GetCount(), "零延迟定时器执行次数")
}

// 高级功能测试

// TestOverflowWheel 测试多级时间轮溢出处理
func TestOverflowWheel(t *testing.T) {
	// 创建一个小的时间轮，容易触发溢出
	tw := NewTimingWheel(100*time.Millisecond, 5) // 总时间跨度 500ms
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tracker := NewExecutionTracker()
	tracker.SetExpected(2)

	// 创建一个超出当前时间轮时间范围的定时器（需要溢出到上级时间轮）
	timer1 := tw.AfterFunc(600*time.Millisecond, func() { // 超出 500ms 范围
		tracker.Track()
	})
	helper.AddTimer(timer1)

	// 创建一个在当前时间轮范围内的定时器
	timer2 := tw.AfterFunc(300*time.Millisecond, func() { // 在 500ms 范围内
		tracker.Track()
	})
	helper.AddTimer(timer2)

	// 等待两个定时器都执行完成
	if !tracker.WaitForCompletion(800 * time.Millisecond) {
		t.Errorf("溢出定时器未在预期时间内执行")
	}

	helper.AssertExecutionCount(2, tracker.GetCount(), "溢出定时器执行次数")
}

// TestLongDelayTimer 测试长延迟定时器
func TestLongDelayTimer(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 10) // 总时间跨度 500ms
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tracker := NewExecutionTracker()
	tracker.SetExpected(1)

	// 创建一个很长的延迟定时器，需要多级时间轮
	timer := tw.AfterFunc(2*time.Second, func() {
		tracker.Track()
	})
	helper.AddTimer(timer)

	// 等待定时器执行（这个测试会比较慢）
	if !tracker.WaitForCompletion(2500 * time.Millisecond) {
		t.Errorf("长延迟定时器未在预期时间内执行")
	}

	helper.AssertExecutionCount(1, tracker.GetCount(), "长延迟定时器执行次数")
}

// TestTimePrecision 测试定时器精度
func TestTimePrecision(t *testing.T) {
	tw := NewTimingWheel(10*time.Millisecond, 100) // 高精度时间轮
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tests := []struct {
		name      string
		delay     time.Duration
		tolerance time.Duration
	}{
		{
			name:      "10ms精度",
			delay:     50 * time.Millisecond,
			tolerance: 20 * time.Millisecond,
		},
		{
			name:      "100ms精度",
			delay:     200 * time.Millisecond,
			tolerance: 30 * time.Millisecond,
		},
		{
			name:      "500ms精度",
			delay:     500 * time.Millisecond,
			tolerance: 50 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewExecutionTracker()
			tracker.SetExpected(1)

			startTime := time.Now()
			timer := tw.AfterFunc(tt.delay, func() {
				tracker.Track()
			})
			helper.AddTimer(timer)

			if !tracker.WaitForCompletion(tt.delay + tt.tolerance) {
				t.Errorf("定时器未在预期时间内执行")
				return
			}

			executions := tracker.GetExecutions()
			if len(executions) > 0 {
				expectedTime := startTime.Add(tt.delay)
				helper.AssertExecutionTime(expectedTime, executions[0], tt.tolerance, "定时器精度")
			}
		})
	}
}

// 并发安全性测试

// TestConcurrentAddTimer 测试并发添加定时器
func TestConcurrentAddTimer(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	const numGoroutines = 10
	const timersPerGoroutine = 20
	const totalTimers = numGoroutines * timersPerGoroutine

	tracker := NewExecutionTracker()
	tracker.SetExpected(totalTimers)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// 并发创建定时器
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < timersPerGoroutine; j++ {
				delay := time.Duration(100+j*10) * time.Millisecond
				timer := tw.AfterFunc(delay, func() {
					tracker.Track()
				})
				helper.AddTimer(timer)
			}
		}(i)
	}

	wg.Wait()

	// 等待所有定时器执行完成
	if !tracker.WaitForCompletion(2 * time.Second) {
		t.Errorf("并发定时器未全部执行完成，期望 %d，实际 %d", totalTimers, tracker.GetCount())
	} else {
		helper.AssertExecutionCount(totalTimers, tracker.GetCount(), "并发定时器执行次数")
	}
}

// TestConcurrentStopTimer 测试并发取消定时器
func TestConcurrentStopTimer(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	const numTimers = 100
	timers := make([]*Timer, numTimers)
	tracker := NewExecutionTracker()

	// 创建多个定时器
	for i := 0; i < numTimers; i++ {
		timer := tw.AfterFunc(1*time.Second, func() {
			tracker.Track()
		})
		timers[i] = timer
		helper.AddTimer(timer)
	}

	var wg sync.WaitGroup
	wg.Add(numTimers)

	// 并发取消定时器
	for i := 0; i < numTimers; i++ {
		go func(index int) {
			defer wg.Done()
			timers[index].Stop()
		}(i)
	}

	wg.Wait()

	// 等待确认没有定时器执行
	time.Sleep(1200 * time.Millisecond)
	if tracker.GetCount() > 0 {
		t.Errorf("应该有 0 个定时器执行，但执行了 %d 个", tracker.GetCount())
	}
}

// TestConcurrentAddAndStop 测试并发添加和取消定时器
func TestConcurrentAddAndStop(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	helper := NewTestHelper(t)
	defer helper.Cleanup()

	tracker := NewExecutionTracker()
	const duration = 500 * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(2)

	// 协程1：不断添加定时器
	go func() {
		defer wg.Done()
		end := time.Now().Add(duration)
		for time.Now().Before(end) {
			timer := tw.AfterFunc(200*time.Millisecond, func() {
				tracker.Track()
			})
			helper.AddTimer(timer)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// 协程2：不断取消定时器
	go func() {
		defer wg.Done()
		end := time.Now().Add(duration)
		for time.Now().Before(end) {
			// 获取一些定时器并尝试取消
			helper.mu.Lock()
			if len(helper.timers) > 0 {
				timer := helper.timers[len(helper.timers)-1]
				helper.mu.Unlock()
				if timer != nil {
					timer.Stop()
				}
			} else {
				helper.mu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

	// 等待剩余定时器执行完成
	time.Sleep(300 * time.Millisecond)
	t.Logf("并发添加取消测试完成，共执行了 %d 个定时器", tracker.GetCount())
}

// 性能基准测试

// BenchmarkTimingWheelAdd 基准测试：添加定时器的性能
func BenchmarkTimingWheelAdd(b *testing.B) {
	tw := NewTimingWheel(100*time.Millisecond, 100)
	tw.Start()
	defer tw.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tw.AfterFunc(time.Duration(i%1000)*time.Millisecond, func() {
			// 空函数
		})
	}
}

// BenchmarkTimingWheelAddAndStop 基准测试：添加和取消定时器的性能
func BenchmarkTimingWheelAddAndStop(b *testing.B) {
	tw := NewTimingWheel(100*time.Millisecond, 100)
	tw.Start()
	defer tw.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timer := tw.AfterFunc(1*time.Second, func() {
			// 空函数
		})
		timer.Stop()
	}
}

// BenchmarkTimingWheelConcurrentAdd 基准测试：并发添加定时器的性能
func BenchmarkTimingWheelConcurrentAdd(b *testing.B) {
	tw := NewTimingWheel(100*time.Millisecond, 100)
	tw.Start()
	defer tw.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tw.AfterFunc(time.Duration(i%1000)*time.Millisecond, func() {
				// 空函数
			})
			i++
		}
	})
}

// BenchmarkTimingWheelExecution 基准测试：定时器执行性能
func BenchmarkTimingWheelExecution(b *testing.B) {
	tw := NewTimingWheel(1*time.Millisecond, 1000)
	tw.Start()
	defer tw.Stop()

	var wg sync.WaitGroup
	wg.Add(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tw.AfterFunc(10*time.Millisecond, func() {
			wg.Done()
		})
	}
	wg.Wait()
}
