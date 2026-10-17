package MapReduce

import "container/heap"

// WorkerMinHeap implements heap.Interface for []*WorkerInfo sorted by Score ascending.

type WorkerMinHeap []*WorkerInfo

func (h WorkerMinHeap) Len() int {
	return len(h)
}

func (h WorkerMinHeap) Less(i, j int) bool {
	return h[i].Score < h[j].Score
}

func (h WorkerMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].HeapIndex = i
	h[j].HeapIndex = j
}

func (h *WorkerMinHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*WorkerInfo)
	item.HeapIndex = n
	*h = append(*h, item)
}

func (h *WorkerMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil       // Avoid memory leak
	item.HeapIndex = -1  // Mark as no longer in heap
	*h = old[0 : n-1]
	return item
}

//
// Helper methods for clean usage with container/heap
//

// PushWorker pushes a worker into the quarantine heap.
func (c *Coordinator) PushQuarantinedWorker(w *WorkerInfo) {
	w.IsQuarantined = true
	heap.Push(&c.quarantineHeap, w)
}

// PopQuarantinedWorker pops and returns the quarantined worker with the lowest Score.
func (c *Coordinator) PopQuarantinedWorker() *WorkerInfo {
	if len(c.quarantineHeap) == 0 {
		return nil
	}
	w := heap.Pop(&c.quarantineHeap).(*WorkerInfo)
	w.IsQuarantined = false
	return w
}

// UpdateWorkerScore updates a worker's score and adjusts its position in the heap if quarantined.
func (c *Coordinator) UpdateWorkerScore(w *WorkerInfo, newScore float64) {
	w.Score = newScore
	if w.IsQuarantined && w.HeapIndex >= 0 {
		heap.Fix(&c.quarantineHeap, w.HeapIndex)
	}
}
