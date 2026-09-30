package timingwheel

import (
	"fmt"
	"sync"
	"time"
)

// TimingWheelManager 时间轮管理器
type TimingWheelManager struct {
	wheel      *TimingWheel
	wheelMutex sync.RWMutex
}

// NewTimingWheelManager 创建新的时间轮管理器
func NewTimingWheelManager() *TimingWheelManager {
	return &TimingWheelManager{}
}

// Initialize 初始化时间轮
func (m *TimingWheelManager) Initialize(tick time.Duration, wheelSize int64) error {
	m.wheelMutex.Lock()
	defer m.wheelMutex.Unlock()

	// 如果已经初始化，直接返回
	if m.wheel != nil {
		return nil
	}

	// 创建新的时间轮实例
	tw := NewTimingWheel(tick, wheelSize)
	if tw == nil {
		return fmt.Errorf("failed to create timing wheel")
	}

	// 启动时间轮
	tw.Start()
	m.wheel = tw
	return nil
}

// GetTimingWheel 安全地获取时间轮实例
func (m *TimingWheelManager) GetTimingWheel() (*TimingWheel, error) {
	m.wheelMutex.RLock()
	defer m.wheelMutex.RUnlock()

	if m.wheel == nil {
		return nil, fmt.Errorf("timing wheel not initialized")
	}
	return m.wheel, nil
}

// Stop 安全地停止时间轮
func (m *TimingWheelManager) Stop() {
	m.wheelMutex.Lock()
	defer m.wheelMutex.Unlock()

	if m.wheel != nil {
		m.wheel.Stop()
		m.wheel = nil
	}
}

// IsInitialized 检查时间轮是否已初始化
func (m *TimingWheelManager) IsInitialized() bool {
	m.wheelMutex.RLock()
	defer m.wheelMutex.RUnlock()
	return m.wheel != nil
}

// AfterFunc 注册一个延迟执行的函数
func (m *TimingWheelManager) AfterFunc(d time.Duration, f func()) (*Timer, error) {
	tw, err := m.GetTimingWheel()
	if err != nil {
		return nil, err
	}
	return tw.AfterFunc(d, f), nil
}

// ScheduleFunc 注册一个按计划执行的函数
func (m *TimingWheelManager) ScheduleFunc(s Scheduler, f func()) (*Timer, error) {
	tw, err := m.GetTimingWheel()
	if err != nil {
		return nil, err
	}
	return tw.ScheduleFunc(s, f), nil
}
