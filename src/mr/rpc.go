package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)

// TaskType is the type of task.
type TaskType int

const (
	// TaskMap is a map task.
	TaskMap TaskType = iota
	// TaskReduce is a reduce task.
	TaskReduce
	// TaskWaiting is a wait task.
	TaskWaiting
	// TaskDone is a done task.ß
	TaskDone
)

// ScheduleType is the type of schedule.
type ScheduleType int

const (
	// ScheduleMap is a map schedule.
	ScheduleMap ScheduleType = iota
	// ScheduleReduce is a reduce schedule.
	ScheduleReduce
	// ScheduleDone is a done schedule.
	ScheduleDone
)

// TaskRequest is the request of GetTask.
type TaskRequest struct{}

// TaskResponse is the response of GetTask.
type TaskResponse struct {
	Status   TaskType
	Index    int
	FileName string
	NReduce  int
	NMap     int
}

// WorkDoneRequest is the request of WorkDone.
type WorkDoneRequest struct {
	Status TaskType
	Index  int
}

// WorkDoneResponse is the response of WorkDone.
type WorkDoneResponse struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	return fmt.Sprintf("/var/tmp/5840-mr-%d", os.Getuid())
}
