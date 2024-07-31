package delayqueue

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// 优先队列的实现
// 参考 https://github.com/nsqio/nsq/blob/master/internal/pqueue/pqueue.go

type item struct {
	Value    interface{}
	Priority int64
	Index    int
}

// 最小堆实现的优先队列，优先级最小的元素在队列的顶部（第 0 位的值为最小值），每次移除元素时总是移除优先级最高（或最低）的元素
type priorityQueue []*item

// 创建一个初始容量为 capacity 的优先队列
func newPriorityQueue(capacity int) priorityQueue {
	return make(priorityQueue, 0, capacity)
}

// 返回优先队列的长度
func (pq priorityQueue) Len() int {
	return len(pq)
}

// 比较两个元素的优先级，返回 true 如果 pq[i] 的优先级小于 pq[j]
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

// 交换优先队列中两个元素的位置，并更新它们的索引
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// 向优先队列中添加一个新元素。如果容量不足，则扩展容量
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(priorityQueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*item)
	item.Index = n
	(*pq)[n] = item
}

// 从优先队列中移除并返回优先级最高的元素。如果队列的长度小于容量的一半且容量大于25，则缩小容量
func (pq *priorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {
		npq := make(priorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

// 查看队列中优先级最高的元素。如果其优先级小于等于 max，则移除该元素并返回；否则返回该元素的优先级与 max 的差值
func (pq *priorityQueue) PeekAndShift(max int64) (*item, int64) {
	if pq.Len() == 0 {
		return nil, 0
	}

	item := (*pq)[0]
	if item.Priority > max {
		return nil, item.Priority - max
	}
	heap.Remove(pq, 0)

	return item, 0
}

// 实现了一个基于优先队列的延迟队列（DelayQueue），其中元素只有在其延迟时间到期后才能被取出。队列的头部总是延迟时间最早过期的元素
type DelayQueue struct {
	C  chan interface{} // 用于输出到期元素的通道
	mu sync.Mutex       // 互斥锁，用于保护优先队列的并发访问
	pq priorityQueue    // 优先队列，用于存储带有延迟时间的元素

	// 类似于 runtime.timers 的睡眠状态
	sleeping int32         // 表示队列是否处于睡眠状态
	wakeupC  chan struct{} // 用于唤醒 Poll 循环的通道
}

// 创建一个指定大小的延迟队列实例
func New(size int) *DelayQueue {
	return &DelayQueue{
		C:       make(chan interface{}),
		pq:      newPriorityQueue(size),
		wakeupC: make(chan struct{}),
	}
}

// 向延迟队列中插入一个新的元素
// 首先创建一个新的 item，然后加锁将该元素插入优先队列
// 如果新插入的元素是队列中延迟时间最早的元素（索引为0），且队列处于睡眠状态，则通过 wakeupC 通道唤醒 Poll 循环
func (dq *DelayQueue) Offer(elem interface{}, expiration int64) {
	item := &item{Value: elem, Priority: expiration}

	dq.mu.Lock()
	heap.Push(&dq.pq, item)
	index := item.Index
	dq.mu.Unlock()

	if index == 0 {
		// 如果新插入的元素是队列中延迟时间最早的元素
		if atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
			dq.wakeupC <- struct{}{}
		}
	}
}

// 启动一个无限循环，不断等待元素到期并将到期的元素发送到通道 C
// 在每次循环中，首先获取当前时间 now，然后尝试从优先队列中取出到期的元素
// 如果没有到期的元素，则设置队列为睡眠状态并等待唤醒或超时。如果有到期的元素，则将其发送到通道 C
func (dq *DelayQueue) Poll(exitC chan struct{}, nowF func() int64) {
	for {
		now := nowF()

		dq.mu.Lock()
		item, delta := dq.pq.PeekAndShift(now)
		if item == nil {
			// 没有元素或至少有一个元素未到期

			// 我们必须保证整个操作的原子性，即
			// 由上面的 PeekAndShift 和下面的 StoreInt32 组成，
			// 以避免 Offer 和 Poll 之间可能出现的竞争条件。
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		if item == nil {
			if delta == 0 {
				// 没有剩余的元素
				select {
				case <-dq.wakeupC:
					// 等待，直到添加新项目
					continue
				case <-exitC:
					goto exit
				}
			} else if delta > 0 {
				// 至少有一个元素未到期
				select {
				case <-dq.wakeupC:
					// 添加一个比当前最早到期时间更早的新项目。
					continue
				case <-time.After(time.Duration(delta) * time.Millisecond):
					// 当前最早项目已过期。
					// 重置睡眠状态，因为无需从 wakeupC 接收
					if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
						// Offer() 的调用者在向 wakeupC 发送消息时被阻塞
						// 耗尽 wakeupC 以解除调用者的阻塞
						<-dq.wakeupC
					}
					continue
				case <-exitC:
					goto exit
				}
			}
		}

		select {
		case dq.C <- item.Value:
			// 过期元素已成功发送出去
		case <-exitC:
			goto exit
		}
	}

exit:
	// 重置状态
	atomic.StoreInt32(&dq.sleeping, 0)
}
