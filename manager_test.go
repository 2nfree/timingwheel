package timingwheel

import (
	"sync"
	"testing"
	"time"
)

// 时间轮管理器测试

// TestNewTimingWheelManager 测试管理器创建
func TestNewTimingWheelManager(t *testing.T) {
	manager := NewTimingWheelManager()
	if manager == nil {
		t.Fatal("NewTimingWheelManager() 返回了 nil")
	}

	// 初始状态应该未初始化
	if manager.IsInitialized() {
		t.Errorf("新创建的管理器不应该是初始化状态")
	}

	// 获取时间轮应该失败
	_, err := manager.GetTimingWheel()
	if err == nil {
		t.Errorf("未初始化的管理器获取时间轮应该失败")
	}
}

// TestTimingWheelManagerInitialize 测试管理器初始化
func TestTimingWheelManagerInitialize(t *testing.T) {
	manager := NewTimingWheelManager()
	defer manager.Stop()

	// 正常初始化
	err := manager.Initialize(100*time.Millisecond, 10)
	if err != nil {
		t.Errorf("管理器初始化失败: %v", err)
	}

	// 检查初始化状态
	if !manager.IsInitialized() {
		t.Errorf("管理器应该处于初始化状态")
	}

	// 获取时间轮应该成功
	tw, err := manager.GetTimingWheel()
	if err != nil {
		t.Errorf("初始化后获取时间轮失败: %v", err)
	}
	if tw == nil {
		t.Errorf("获取的时间轮是 nil")
	}

	// 重复初始化应该成功（幂等性）
	err = manager.Initialize(200*time.Millisecond, 20)
	if err != nil {
		t.Errorf("重复初始化应该成功: %v", err)
	}

	// 验证时间轮参数没有改变（第一次初始化的参数保持不变）
	tw2, _ := manager.GetTimingWheel()
	if tw != tw2 {
		t.Errorf("重复初始化不应该创建新的时间轮实例")
	}
}

// TestTimingWheelManagerStop 测试管理器停止
func TestTimingWheelManagerStop(t *testing.T) {
	manager := NewTimingWheelManager()

	// 初始化管理器
	err := manager.Initialize(100*time.Millisecond, 10)
	if err != nil {
		t.Errorf("管理器初始化失败: %v", err)
	}

	// 验证初始化状态
	if !manager.IsInitialized() {
		t.Errorf("管理器应该处于初始化状态")
	}

	// 停止管理器
	manager.Stop()

	// 验证停止后状态
	if manager.IsInitialized() {
		t.Errorf("停止后管理器不应该处于初始化状态")
	}

	// 获取时间轮应该失败
	_, err = manager.GetTimingWheel()
	if err == nil {
		t.Errorf("停止后获取时间轮应该失败")
	}

	// 多次停止应该安全
	manager.Stop()
	manager.Stop()
}

// TestTimingWheelManagerAfterFunc 测试管理器的 AfterFunc 方法
func TestTimingWheelManagerAfterFunc(t *testing.T) {
	manager := NewTimingWheelManager()
	defer manager.Stop()

	// 未初始化时调用 AfterFunc 应该失败
	_, err := manager.AfterFunc(100*time.Millisecond, func() {})
	if err == nil {
		t.Errorf("未初始化时调用 AfterFunc 应该失败")
	}

	// 初始化管理器
	err = manager.Initialize(50*time.Millisecond, 20)
	if err != nil {
		t.Errorf("管理器初始化失败: %v", err)
	}

	// 测试 AfterFunc
	tracker := NewExecutionTracker()
	tracker.SetExpected(1)

	timer, err := manager.AfterFunc(200*time.Millisecond, func() {
		tracker.Track()
	})
	if err != nil {
		t.Errorf("AfterFunc 调用失败: %v", err)
	}
	if timer == nil {
		t.Errorf("AfterFunc 返回了 nil 定时器")
	}

	// 等待执行完成
	if !tracker.WaitForCompletion(300 * time.Millisecond) {
		t.Errorf("定时器未在预期时间内执行")
	}

	// 验证执行次数
	if tracker.GetCount() != 1 {
		t.Errorf("定时器执行次数不正确，期望 1，实际 %d", tracker.GetCount())
	}
}

// TestTimingWheelManagerScheduleFunc 测试管理器的 ScheduleFunc 方法
func TestTimingWheelManagerScheduleFunc(t *testing.T) {
	manager := NewTimingWheelManager()
	defer manager.Stop()

	// 未初始化时调用 ScheduleFunc 应该失败
	scheduler := NewIntervalScheduler(100 * time.Millisecond)
	_, err := manager.ScheduleFunc(scheduler, func() {})
	if err == nil {
		t.Errorf("未初始化时调用 ScheduleFunc 应该失败")
	}

	// 初始化管理器
	err = manager.Initialize(50*time.Millisecond, 20)
	if err != nil {
		t.Errorf("管理器初始化失败: %v", err)
	}

	// 测试 ScheduleFunc
	tracker := NewExecutionTracker()
	tracker.SetExpected(2)

	var count int64
	var timer *Timer
	timer, err = manager.ScheduleFunc(scheduler, func() {
		tracker.Track()
		count++
		// 执行2次后停止
		if count >= 2 {
			timer.Stop()
		}
	})
	if err != nil {
		t.Errorf("ScheduleFunc 调用失败: %v", err)
	}
	if timer == nil {
		t.Errorf("ScheduleFunc 返回了 nil 定时器")
	}

	// 等待执行完成
	if !tracker.WaitForCompletion(300 * time.Millisecond) {
		t.Errorf("调度器未在预期时间内执行")
	}

	// 验证执行次数
	if tracker.GetCount() < 2 {
		t.Errorf("调度器执行次数不足，期望至少 2，实际 %d", tracker.GetCount())
	}
}

// TestTimingWheelManagerConcurrency 测试管理器的并发安全性
func TestTimingWheelManagerConcurrency(t *testing.T) {
	manager := NewTimingWheelManager()
	defer manager.Stop()

	const numGoroutines = 10
	var wg sync.WaitGroup

	// 并发初始化
	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			if err := manager.Initialize(50*time.Millisecond, 20); err != nil {
				t.Errorf("并发初始化失败: %v", err)
				return
			}
		}()
	}
	wg.Wait()

	// 验证只有一个实例被创建
	if !manager.IsInitialized() {
		t.Errorf("并发初始化后管理器应该处于初始化状态")
	}

	tw1, _ := manager.GetTimingWheel()

	// 并发获取时间轮
	timingWheels := make([]*TimingWheel, numGoroutines)
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(index int) {
			defer wg.Done()
			tw, _ := manager.GetTimingWheel()
			timingWheels[index] = tw
		}(i)
	}
	wg.Wait()

	// 验证所有获取的实例都是同一个
	for i, tw := range timingWheels {
		if tw != tw1 {
			t.Errorf("并发获取的时间轮实例[%d]不一致", i)
		}
	}

	// 并发创建定时器
	tracker := NewExecutionTracker()
	tracker.SetExpected(numGoroutines)

	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			if _, err := manager.AfterFunc(100*time.Millisecond, func() {
				tracker.Track()
			}); err != nil {
				t.Errorf("并发创建定时器失败: %v", err)
			}
		}()
	}
	wg.Wait()

	// 等待所有定时器执行完成
	if !tracker.WaitForCompletion(300 * time.Millisecond) {
		t.Errorf("并发定时器未全部执行完成，执行了 %d/%d", tracker.GetCount(), numGoroutines)
	}
}

// TestTimingWheelManagerStopWithActiveTimers 测试有活跃定时器时停止管理器
func TestTimingWheelManagerStopWithActiveTimers(t *testing.T) {
	manager := NewTimingWheelManager()

	err := manager.Initialize(50*time.Millisecond, 20)
	if err != nil {
		t.Fatalf("管理器初始化失败: %v", err)
	}

	const numTimers = 5
	executed := make(chan struct{}, numTimers)

	for range numTimers {
		_, _ = manager.AfterFunc(2*time.Second, func() {
			executed <- struct{}{}
		})
	}

	manager.Stop()

	if manager.IsInitialized() {
		t.Errorf("停止后管理器不应该处于初始化状态")
	}

	time.Sleep(1 * time.Second)

	select {
	case <-executed:
		t.Errorf("有定时器任务被执行，应该在 Stop() 后全部取消")
	default:
		// 成功，未触发任务
	}
}

// TestTimingWheelManagerReInitialize 测试重新初始化
func TestTimingWheelManagerReInitialize(t *testing.T) {
	manager := NewTimingWheelManager()

	// 第一次初始化
	err := manager.Initialize(100*time.Millisecond, 10)
	if err != nil {
		t.Errorf("第一次初始化失败: %v", err)
	}

	tw1, _ := manager.GetTimingWheel()

	// 停止管理器
	manager.Stop()

	// 重新初始化
	err = manager.Initialize(50*time.Millisecond, 20)
	if err != nil {
		t.Errorf("重新初始化失败: %v", err)
	}

	// 验证是新的实例
	tw2, _ := manager.GetTimingWheel()
	if tw1 == tw2 {
		t.Errorf("重新初始化应该创建新的时间轮实例")
	}

	// 验证新实例正常工作
	tracker := NewExecutionTracker()
	tracker.SetExpected(1)

	_, err = manager.AfterFunc(100*time.Millisecond, func() {
		tracker.Track()
	})
	if err != nil {
		t.Errorf("重新初始化后 AfterFunc 失败: %v", err)
	}

	if !tracker.WaitForCompletion(200 * time.Millisecond) {
		t.Errorf("重新初始化后定时器未执行")
	}

	manager.Stop()
}
