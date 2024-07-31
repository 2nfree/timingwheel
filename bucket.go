package timingwheel

import (
	"container/list"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Timer 代表单个事件。当 Timer 到期时，将执行给定的任务
type Timer struct {
	expiration int64  // 到期时间（以毫秒为单位）
	task       func() // 定时任务

	// 指向包含该定时器的 bucket
	b unsafe.Pointer // 类型：*bucket

	// 定时器在双向链表中的元素
	element *list.Element
}

// 获取当前定时器所属的 bucket
func (t *Timer) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

// 设置当前定时器所属的 bucket
func (t *Timer) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

// 停止定时器，如果定时器已过期或已停止，返回 false, 否则返回 true
// 通过不断重试获取和移除定时器所属的 bucket，确保定时器被正确移除
func (t *Timer) Stop() bool {
	stopped := false
	for b := t.getBucket(); b != nil; b = t.getBucket() {
		stopped = b.Remove(t)
	}
	return stopped
}

type bucket struct {
	expiration int64      // 桶的过期时间
	mu         sync.Mutex // 互斥锁，保护定时器链表的并发访问
	timers     *list.List // 包含的定时器的双向链表
}

// 创建一个新的 bucket
func newBucket() *bucket {
	return &bucket{
		timers:     list.New(),
		expiration: -1,
	}
}

// 获取桶的过期时间
func (b *bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

// 设置桶的过期时间，返回是否成功更新
func (b *bucket) SetExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}

// 向桶中添加定时器
func (b *bucket) Add(t *Timer) {
	b.mu.Lock()
	e := b.timers.PushBack(t)
	t.setBucket(b)
	t.element = e

	b.mu.Unlock()
}

// 从桶中移除定时器，返回是否成功移除
func (b *bucket) remove(t *Timer) bool {
	if t.getBucket() != b {
		return false
	}
	b.timers.Remove(t.element)
	t.setBucket(nil)
	t.element = nil
	return true
}

// 安全地从桶中移除定时器
func (b *bucket) Remove(t *Timer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

// 刷新桶中的定时器，将它们重新插入到新的桶中或执行它们
func (b *bucket) Flush(reinsert func(*Timer)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for e := b.timers.Front(); e != nil; {
		next := e.Next()
		t := e.Value.(*Timer)
		b.remove(t)
		reinsert(t)
		e = next
	}
	b.SetExpiration(-1)
}
