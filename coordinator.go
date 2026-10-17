package MapReduce

import (
	"container/heap"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//


func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if(args.WorkerID==-1) {
		args.WorkerID = registerWorker(c)
	}
	reply.WorkerID = args.WorkerID
	
	if c.phase == DonePhase {
		reply.TaskType="EXIT"
		return nil
	}

	endIdx := c.nMap
	if c.phase == ReducePhase {
		endIdx = c.nMap+c.nReduce
	}

	now := time.Now()

	if c.nextUnassignedIdx < endIdx {
		task := &c.allTasks[c.nextUnassignedIdx]
		task.Status = TaskInProgress
		task.WorkerID = args.WorkerID
		task.StartTime = now

		c.inFlightQueue = append(c.inFlightQueue, task.GlobTaskID)
		c.nextUnassignedIdx++

		c.populateTaskReply(task, reply)
		return nil
	}

	for len(c.inFlightQueue) > 0 {
		headID := c.inFlightQueue[0]
		task := &c.allTasks[headID]

		if task.Status != TaskInProgress {
			c.inFlightQueue = c.inFlightQueue[1:]
			continue
		}

		if now.Sub(task.StartTime) > 10*time.Second {
			c.inFlightQueue = c.inFlightQueue[1:]
			
			oldWorkerID := task.WorkerID
			
			if w, exsists := c.workers[oldWorkerID]; exsists {
				tFail := float64(time.Since(c.bootTime).Milliseconds()) / 100.0
				w.Score += math.Pow(tFail+1.0, c.heap_alpha)
				c.quarantineWokerLocked(w)
			}

			task.WorkerID = args.WorkerID
			task.StartTime = now

			c.inFlightQueue = append(c.inFlightQueue, headID)
			
			c.populateTaskReply(task, reply)
			return nil
		}
		break
	}

	reply.TaskType="WAIT"
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	globTaskID := args.TaskID
	if args.TaskType == "REDUCE" {
		globTaskID += c.nMap
	}

	task  := &c.allTasks[globTaskID]
	reply.Accepted = true

	if task.Status == TaskCompleted {
		return nil
	}

	if args.Status {
		task.Status = TaskCompleted

		if w, exsists := c.workers[args.WorkerID]; exsists {
			w.Score *= c.heap_forgive
		}

		endIdx := c.nMap
		if c.phase == ReducePhase {
			endIdx = c.nMap + c.nReduce
		}

		for c.minUnfinishedIdx <endIdx && c.allTasks[c.minUnfinishedIdx].Status==TaskCompleted {
			c.minUnfinishedIdx++
		}

		if c.minUnfinishedIdx == endIdx {
			if c.phase == MapPhase {
				c.phase = ReducePhase
				c.nextUnassignedIdx = c.nMap
				c.minUnfinishedIdx = c.nMap
			} else if c.phase == ReducePhase {
				c.phase = DonePhase
			}
		}
	} else {
		task.Status = TaskUnassigned
		if w, exsists := c.workers[args.WorkerID]; exsists {
			tFail := float64(time.Since(c.bootTime).Milliseconds())/100.0
			w.Score += math.Pow(tFail+1.0, c.heap_alpha)
			c.quarantineWokerLocked(w)
		}
	}

	return nil
}

func (c *Coordinator) populateTaskReply(task *Task, reply *TaskReply) {
	reply.TaskID = task.TaskID
	reply.TaskType = task.TaskType
	reply.Files = task.Files
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap
}

func (c *Coordinator) quarantineWokerLocked(w *WorkerInfo) {
	if !w.IsQuarantined {
		w.IsQuarantined = true
		heap.Push(&c.quarantineHeap, w)
	} else {
		heap.Fix(&c.quarantineHeap, w.HeapIndex)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == DonePhase
}

func registerWorker(c* Coordinator) int {
	w_id := c.workerCount
	c.workerCount++

	c.workers[w_id] = &WorkerInfo{
		WorkerID: w_id,
		Score: 0,
		IsQuarantined: false,
		HeapIndex: -1,
	}

	return w_id
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: 		nReduce,
		nMap : 			len(files),
		phase : 		MapPhase,
		workers : 		make(map[int]*WorkerInfo),
		quarantineHeap: make(WorkerMinHeap, 0),
		bootTime: 		time.Now(),
		allTasks: 		make([]Task, len(files)+nReduce),
		heap_alpha: 	0.9,
		heap_forgive: 	0.9,
	}

	c.mu.Lock()

	// Your code here.

	for i, file := range files {
		c.allTasks[i] = Task{
			GlobTaskID: i,
			TaskID: i,
			TaskType: "MAP",
			Files: []string{file},
			Status: TaskUnassigned,
			WorkerID: -1,
		}
	}

	for i:=0; i<nReduce; i++ {
		c.allTasks[i+c.nMap] = Task{
			GlobTaskID: c.nMap+i,
			TaskID: i,
			TaskType: "REDUCE",
			Status: TaskUnassigned,
			WorkerID: -1,
		}
	}

	for r:=0; r<c.nReduce; r++ {
		globalID := c.nMap+r
		task := &c.allTasks[globalID]

		task.Files = make([]string, 0, c.nMap)

		for m:=0; m<c.nMap; m++ {
			fileName := fmt.Sprintf("mr-%d-%d", m, r)
			task.Files = append(task.Files, fileName)
		}
	}

	heap.Init(&c.quarantineHeap)
	c.mu.Unlock()

	c.server()
	return &c
}
