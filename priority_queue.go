package timingwheel

import "container/heap"

// 优先队列的实现
// 参考 https://github.com/nsqio/nsq/blob/master/internal/pqueue/pqueue.go

type item struct {
	Value    any
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

// 向优先队列中添加一个新元素，如果容量不足，则扩展容量
func (pq *priorityQueue) Push(x any) {
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

// 从优先队列中移除并返回优先级最高的元素，如果队列的长度小于容量的一半且容量大于25，则缩小容量
func (pq *priorityQueue) Pop() any {
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

// 查看队列中优先级最高的元素，如果其优先级小于等于 max，则移除该元素并返回；否则返回该元素的优先级与 max 的差值
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
