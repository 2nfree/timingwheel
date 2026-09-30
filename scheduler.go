package timingwheel

import (
	"time"

	"github.com/robfig/cron/v3"
)

// 默认调度器
var (
	EverySecond = NewIntervalScheduler(time.Second)        // 每秒
	EveryMinute = NewIntervalScheduler(time.Minute)        // 每分钟
	EveryHour   = NewIntervalScheduler(time.Hour)          // 每小时
	EveryDay    = NewDailyScheduler(0, 0, 0)               // 每天 0:00:00
	EveryWeek   = NewWeeklyScheduler(time.Sunday, 0, 0, 0) // 每周日 0:00:00
)

type (
	// 固定间隔调度器
	IntervalScheduler struct {
		Interval time.Duration
	}

	// 每天固定时间调度器
	DailyScheduler struct {
		Hour   int // 0-23
		Minute int // 0-59
		Second int // 0-59
	}

	// 每周固定时间调度器
	WeeklyScheduler struct {
		Weekday time.Weekday // 周几 (0-6, 0=周日)
		Hour    int          // 0-23
		Minute  int          // 0-59
		Second  int          // 0-59
	}

	// 定时任务表达式调度器
	CronScheduler struct {
		schedule cron.Schedule
	}
)

// NewIntervalScheduler 创建一个固定间隔调度器
func NewIntervalScheduler(interval time.Duration) *IntervalScheduler {
	return &IntervalScheduler{Interval: interval}
}

// IntervalScheduler 实现
func (s *IntervalScheduler) Next(prev time.Time) time.Time {
	if prev.IsZero() {
		return time.Now().Add(s.Interval)
	}
	return prev.Add(s.Interval)
}

// NewDailyScheduler 创建一个每天固定时间调度器
func NewDailyScheduler(hour, minute, second int) *DailyScheduler {
	return &DailyScheduler{Hour: hour, Minute: minute, Second: second}
}

// DailyScheduler 实现
func (s *DailyScheduler) Next(prev time.Time) time.Time {
	if prev.IsZero() {
		prev = time.Now()
	}
	next := time.Date(
		prev.Year(), prev.Month(), prev.Day(),
		s.Hour, s.Minute, s.Second, 0,
		prev.Location(),
	)

	if !next.After(prev) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

// NewWeeklyScheduler 创建一个每周固定时间调度器
func NewWeeklyScheduler(weekday time.Weekday, hour, minute, second int) *WeeklyScheduler {
	return &WeeklyScheduler{Weekday: weekday, Hour: hour, Minute: minute, Second: second}
}

// WeeklyScheduler 实现
func (s *WeeklyScheduler) Next(prev time.Time) time.Time {
	if prev.IsZero() {
		prev = time.Now()
	}
	// 计算今天的目标时间点
	targetToday := time.Date(
		prev.Year(), prev.Month(), prev.Day(),
		s.Hour, s.Minute, s.Second, 0, prev.Location(),
	)

	// 计算当前是周几
	currentWeekday := prev.Weekday()
	daysUntilTarget := int(s.Weekday - currentWeekday)
	if daysUntilTarget < 0 {
		daysUntilTarget += 7
	}

	// 如果今天就是目标星期几
	if currentWeekday == s.Weekday {
		// 如果当前时间在今天的执行时间之前，就返回今天的时间
		if prev.Before(targetToday) {
			return targetToday
		}
		// 否则返回下周的时间
		daysUntilTarget = 7
	}

	return targetToday.AddDate(0, 0, daysUntilTarget)
}

// NewCronScheduler 创建一个 CronScheduler 实例
func NewCronScheduler(expr string) (*CronScheduler, error) {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(expr)
	if err != nil {
		return nil, err
	}
	return &CronScheduler{schedule: sched}, nil
}

// CronScheduler 实现
func (s *CronScheduler) Next(prev time.Time) time.Time {
	if prev.IsZero() {
		return s.schedule.Next(time.Now())
	}
	return s.schedule.Next(prev)
}
