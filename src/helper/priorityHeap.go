package helper

// Object represents a target with a priority (lower = higher priority)
type Object struct {
	X        int
	Y        int
	Priority int // Lower values are higher priority
}

// ObjectHeap is a min-heap of Objects
type ObjectHeap []Object

func (h ObjectHeap) Len() int { return len(h) }

func (h ObjectHeap) Less(i, j int) bool {
	return h[i].Priority < h[j].Priority
}

func (h ObjectHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ObjectHeap) Push(x interface{}) {
	*h = append(*h, x.(Object))
}

func (h *ObjectHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
