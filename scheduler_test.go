package timingwheel

import (
	"sync/atomic"
	"testing"
	"time"
)

// 调度器测试

// TestIntervalScheduler 测试固定间隔调度器
func TestIntervalScheduler(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		prev     time.Time
		wantNext bool
	}{
		{
			name:     "1秒间隔_零时间",
			interval: 1 * time.Second,
			prev:     time.Time{},
			wantNext: true,
		},
		{
			name:     "1秒间隔_有前置时间",
			interval: 1 * time.Second,
			prev:     time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			wantNext: true,
		},
		{
			name:     "100毫秒间隔",
			interval: 100 * time.Millisecond,
			prev:     time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			wantNext: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := NewIntervalScheduler(tt.interval)
			next := scheduler.Next(tt.prev)

			if tt.wantNext && next.IsZero() {
				t.Errorf("IntervalScheduler.Next() 返回了零时间，期望非零时间")
				return
			}

			if !tt.wantNext && !next.IsZero() {
				t.Errorf("IntervalScheduler.Next() 返回了非零时间，期望零时间")
				return
			}

			if tt.wantNext {
				var expectedNext time.Time
				if tt.prev.IsZero() {
					// 应该是当前时间 + 间隔
					expectedNext = time.Now().Add(tt.interval)
					// 允许一定的时间误差
					if diff := next.Sub(expectedNext); diff < -100*time.Millisecond || diff > 100*time.Millisecond {
						t.Errorf("IntervalScheduler.Next() 时间误差过大，期望约 %v，得到 %v，差异 %v",
							expectedNext, next, diff)
					}
				} else {
					// 应该是前一次时间 + 间隔
					expectedNext = tt.prev.Add(tt.interval)
					if !next.Equal(expectedNext) {
						t.Errorf("IntervalScheduler.Next() = %v, 期望 %v", next, expectedNext)
					}
				}
			}
		})
	}
}

// TestDailyScheduler 测试每天固定时间调度器
func TestDailyScheduler(t *testing.T) {
	tests := []struct {
		name         string
		hour         int
		minute       int
		second       int
		prev         time.Time
		expectedHour int
		expectedMin  int
		expectedSec  int
		shouldBeNext bool
	}{
		{
			name:         "每天9点_当前8点",
			hour:         9,
			minute:       0,
			second:       0,
			prev:         time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC),
			expectedHour: 9,
			expectedMin:  0,
			expectedSec:  0,
			shouldBeNext: false, // 今天的9点
		},
		{
			name:         "每天9点_当前10点",
			hour:         9,
			minute:       0,
			second:       0,
			prev:         time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			expectedHour: 9,
			expectedMin:  0,
			expectedSec:  0,
			shouldBeNext: true, // 明天的9点
		},
		{
			name:         "每天23点30分_当前23点20分",
			hour:         23,
			minute:       30,
			second:       0,
			prev:         time.Date(2024, 1, 1, 23, 20, 0, 0, time.UTC),
			expectedHour: 23,
			expectedMin:  30,
			expectedSec:  0,
			shouldBeNext: false, // 今天的23:30
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := NewDailyScheduler(tt.hour, tt.minute, tt.second)
			next := scheduler.Next(tt.prev)

			if next.IsZero() {
				t.Errorf("DailyScheduler.Next() 返回了零时间")
				return
			}

			// 验证时分秒
			if next.Hour() != tt.expectedHour || next.Minute() != tt.expectedMin || next.Second() != tt.expectedSec {
				t.Errorf("DailyScheduler.Next() 时间不正确，期望 %d:%d:%d，得到 %d:%d:%d",
					tt.expectedHour, tt.expectedMin, tt.expectedSec,
					next.Hour(), next.Minute(), next.Second())
			}

			// 验证日期
			if tt.shouldBeNext {
				// 应该是第二天
				expectedDate := tt.prev.Add(24 * time.Hour)
				if next.Year() != expectedDate.Year() || next.Month() != expectedDate.Month() || next.Day() != expectedDate.Day() {
					t.Errorf("DailyScheduler.Next() 日期不正确，期望第二天，得到 %v", next)
				}
			} else {
				// 应该是同一天
				if next.Year() != tt.prev.Year() || next.Month() != tt.prev.Month() || next.Day() != tt.prev.Day() {
					t.Errorf("DailyScheduler.Next() 日期不正确，期望同一天，得到 %v", next)
				}
			}
		})
	}
}

// TestWeeklyScheduler 测试每周固定时间调度器
func TestWeeklyScheduler(t *testing.T) {
	tests := []struct {
		name         string
		weekday      time.Weekday
		hour         int
		minute       int
		second       int
		prev         time.Time
		expectedDays int // 期望相对于prev的天数差
	}{
		{
			name:         "每周一9点_当前周一8点",
			weekday:      time.Monday,
			hour:         9,
			minute:       0,
			second:       0,
			prev:         time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC), // 2024-01-01 是周一
			expectedDays: 0,                                           // 当天
		},
		{
			name:         "每周一9点_当前周一10点",
			weekday:      time.Monday,
			hour:         9,
			minute:       0,
			second:       0,
			prev:         time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), // 2024-01-01 是周一
			expectedDays: 7,                                            // 下周一
		},
		{
			name:         "每周五18点_当前周一10点",
			weekday:      time.Friday,
			hour:         18,
			minute:       0,
			second:       0,
			prev:         time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), // 2024-01-01 是周一
			expectedDays: 4,                                            // 本周五
		},
		{
			name:         "每周日0点_当前周六23点",
			weekday:      time.Sunday,
			hour:         0,
			minute:       0,
			second:       0,
			prev:         time.Date(2024, 1, 6, 23, 0, 0, 0, time.UTC), // 2024-01-06 是周六
			expectedDays: 1,                                            // 明天周日
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := NewWeeklyScheduler(tt.weekday, tt.hour, tt.minute, tt.second)
			next := scheduler.Next(tt.prev)

			if next.IsZero() {
				t.Errorf("WeeklyScheduler.Next() 返回了零时间")
				return
			}

			// 验证星期几
			if next.Weekday() != tt.weekday {
				t.Errorf("WeeklyScheduler.Next() 星期不正确，期望 %v，得到 %v",
					tt.weekday, next.Weekday())
			}

			// 验证时分秒
			if next.Hour() != tt.hour || next.Minute() != tt.minute || next.Second() != tt.second {
				t.Errorf("WeeklyScheduler.Next() 时间不正确，期望 %d:%d:%d，得到 %d:%d:%d",
					tt.hour, tt.minute, tt.second,
					next.Hour(), next.Minute(), next.Second())
			}

			// 验证日期差
			expectedNext := tt.prev.AddDate(0, 0, tt.expectedDays)
			expectedNext = time.Date(expectedNext.Year(), expectedNext.Month(), expectedNext.Day(),
				tt.hour, tt.minute, tt.second, 0, expectedNext.Location())

			if !next.Equal(expectedNext) {
				t.Errorf("WeeklyScheduler.Next() 日期不正确，期望 %v，得到 %v",
					expectedNext, next)
			}
		})
	}
}

// TestCronScheduler 测试 Cron 表达式调度器
func TestCronScheduler(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{
			name:    "每分钟执行",
			expr:    "0 * * * * *", // 每分钟的第0秒
			wantErr: false,
		},
		{
			name:    "每天9点执行",
			expr:    "0 0 9 * * *", // 每天9:00:00
			wantErr: false,
		},
		{
			name:    "每周一9点执行",
			expr:    "0 0 9 * * 1", // 每周一9:00:00
			wantErr: false,
		},
		{
			name:    "每月1号执行",
			expr:    "0 0 0 1 * *", // 每月1号0:00:00
			wantErr: false,
		},
		{
			name:    "无效表达式",
			expr:    "invalid",
			wantErr: true,
		},
		{
			name:    "空表达式",
			expr:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler, err := NewCronScheduler(tt.expr)

			if tt.wantErr {
				if err == nil {
					t.Errorf("NewCronScheduler() 期望错误，但没有得到错误")
				}
				return
			}

			if err != nil {
				t.Errorf("NewCronScheduler() 意外错误: %v", err)
				return
			}

			if scheduler == nil {
				t.Errorf("NewCronScheduler() 返回了 nil")
				return
			}

			// 测试 Next 方法
			prev := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			next := scheduler.Next(prev)

			if next.IsZero() {
				t.Errorf("CronScheduler.Next() 返回了零时间")
				return
			}

			if !next.After(prev) {
				t.Errorf("CronScheduler.Next() 返回的时间不在预期的时间之后，prev: %v, next: %v", prev, next)
			}

			// 测试零时间输入
			nextFromZero := scheduler.Next(time.Time{})
			if nextFromZero.IsZero() {
				t.Errorf("CronScheduler.Next() 对零时间输入返回了零时间")
			}
		})
	}
}

// TestScheduleFunc 测试调度函数功能
func TestScheduleFunc(t *testing.T) {
	tw := NewTimingWheel(50*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	tracker := NewExecutionTracker()
	tracker.SetExpected(3) // 期望执行3次

	// 使用间隔调度器，每200ms执行一次
	scheduler := NewIntervalScheduler(200 * time.Millisecond)

	// 创建一个计数器来控制执行次数
	var count int64
	var timer *Timer
	timer = tw.ScheduleFunc(scheduler, func() {
		tracker.Track()
		atomic.AddInt64(&count, 1)

		// 执行3次后停止定时器
		if atomic.LoadInt64(&count) >= 3 {
			timer.Stop()
		}
	})

	if timer == nil {
		t.Fatal("ScheduleFunc 返回了 nil 定时器")
	}

	// 等待执行完成
	if !tracker.WaitForCompletion(800 * time.Millisecond) {
		t.Errorf("调度函数未在预期时间内执行完成，执行了 %d 次", tracker.GetCount())
	}

	// 验证执行次数
	if tracker.GetCount() < 3 {
		t.Errorf("调度函数执行次数不足，期望至少 3 次，实际 %d 次", tracker.GetCount())
	}

	// 再等待一段时间，确保定时器已停止
	time.Sleep(300 * time.Millisecond)
	finalCount := tracker.GetCount()
	if finalCount > 3 {
		t.Errorf("定时器停止后仍在执行，最终执行了 %d 次", finalCount)
	}
}

// TestPreDefinedSchedulers 测试预定义的调度器
func TestPreDefinedSchedulers(t *testing.T) {
	schedulers := []struct {
		name      string
		scheduler Scheduler
	}{
		{"EverySecond", EverySecond},
		{"EveryMinute", EveryMinute},
		{"EveryHour", EveryHour},
		{"EveryDay", EveryDay},
		{"EveryWeek", EveryWeek},
	}

	for _, s := range schedulers {
		t.Run(s.name, func(t *testing.T) {
			if s.scheduler == nil {
				t.Errorf("预定义调度器 %s 是 nil", s.name)
				return
			}

			// 测试 Next 方法
			prev := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
			next := s.scheduler.Next(prev)

			if next.IsZero() {
				t.Errorf("预定义调度器 %s.Next() 返回了零时间", s.name)
				return
			}

			if !next.After(prev) {
				t.Errorf("预定义调度器 %s.Next() 返回的时间不在预期的时间之后", s.name)
			}
		})
	}
}
