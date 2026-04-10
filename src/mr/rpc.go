package mr

//
// RPC definitions.
//

import (
	"fmt"
	"os"
)

// TaskType is the type of task assigned to a worker.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// TaskRequest is the request for GetTask RPC.
type TaskRequest struct{}

// TaskResponse is the response for GetTask RPC.
type TaskResponse struct {
	TaskType TaskType
	TaskId   int
	FileName string
	NReduce  int
	NMap     int
}

// DoneRequest is the request for TaskDone RPC.
type DoneRequest struct {
	TaskType TaskType
	TaskId   int
}

// DoneResponse is the response for TaskDone RPC.
type DoneResponse struct{}

// coordinatorSock returns a unique UNIX-domain socket name for the coordinator.
func coordinatorSock() string {
	return fmt.Sprintf("/var/tmp/5840-mr-%d", os.Getuid())
}
