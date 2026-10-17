package MapReduce

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type TaskArgs struct {
	WorkerID int
}

type TaskReply struct {
	TaskType string
	TaskID int
	Files []string
	NReduce int
	NMap int
	WorkerID int
}

type ReportTaskArgs struct {
	TaskType string
	TaskID int
	WorkerID int
	Status bool
}

type ReportTaskReply struct {
	Accepted bool
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
