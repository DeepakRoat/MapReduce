package MapReduce

import (
	"sync"
	"time"
)

type TaskStatus int

const (
	TaskUnassigned TaskStatus = iota
	TaskInProgress
	TaskCompleted
)

type JobPhase int

const (
	MapPhase JobPhase = iota
	ReducePhase
	DonePhase
)

type Task struct {
	TaskID    int
	GlobTaskID int
	TaskType  string
	Files     []string
	Status    TaskStatus
	WorkerID  int
	StartTime time.Time
}

type WorkerInfo struct {
	WorkerID      int
	Score         float64
	IsQuarantined bool
	HeapIndex     int
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	nReduce int
	nMap int
	phase   JobPhase

	allTasks    []Task
	inFlightQueue []int

	workers        map[int]*WorkerInfo
	quarantineHeap WorkerMinHeap
	bootTime       time.Time

	workerCount int
	minUnfinishedIdx int
	nextUnassignedIdx int

	heap_alpha float64
	heap_forgive float64
}

type KeyValue struct {
	Key   string
	Value string
}