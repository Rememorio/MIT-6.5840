package mr

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Coordinator is a server that listens for RPCs from worker.go
type Coordinator struct {
	Lock            sync.Mutex
	taskID2FileName map[int]string
	FileName        []string
	IdleTask        []int             // The tasks that are waiting to be assigned.
	ProcessingTask  map[int]time.Time // The tasks that are in process.
	Status          ScheduleType
	NReduce         int
	NMap            int
}

func (c *Coordinator) GetTask(_ *TaskRequest, rsp *TaskResponse) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	switch c.Status {
	case ScheduleDone:
		rsp.Status = TaskDone

	case ScheduleMap:
		if len(c.IdleTask) == 0 {
			c.detectFail()
		}
		if len(c.IdleTask) == 0 {
			rsp.Status = TaskWaiting
			return nil
		}
		// Get an idle task.
		taskID := c.IdleTask[0]
		c.IdleTask = c.IdleTask[1:]
		c.ProcessingTask[taskID] = time.Now()

		rsp.FileName = c.taskID2FileName[taskID]
		rsp.Index = taskID
		rsp.Status = TaskMap
		rsp.NReduce = c.NReduce

	case ScheduleReduce:
		if len(c.IdleTask) == 0 {
			c.detectFail()
		}
		if len(c.IdleTask) == 0 {
			rsp.Status = TaskWaiting
			return nil
		}
		// Get an idle task.
		taskID := c.IdleTask[0]
		c.IdleTask = c.IdleTask[1:]
		c.ProcessingTask[taskID] = time.Now()

		rsp.NMap = c.NMap
		rsp.Index = taskID
		rsp.Status = TaskReduce

	default:
		return fmt.Errorf("unknown schedule type: %d", c.Status)
	}

	return nil
}

// timeout is the time that a processing task can be considered as failed.
const timeout = 10 * time.Second

// detectFail detect the failed tasks and cancel them.
func (c *Coordinator) detectFail() {
	for i, start := range c.ProcessingTask {
		if time.Since(start) > timeout {
			// Cancel the task.
			c.IdleTask = append(c.IdleTask, i)
			delete(c.ProcessingTask, i)
		}
	}
}

// WorkDone is called by worker.go to report the status of a task.
func (c *Coordinator) WorkDone(req *WorkDoneRequest, _ *WorkDoneResponse) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	switch req.Status {
	case TaskMap:
		if c.Status == ScheduleMap {
			delete(c.ProcessingTask, req.Index)
			if len(c.IdleTask) == 0 && len(c.ProcessingTask) == 0 {
				c.IdleTask = make([]int, c.NReduce)
				for i := 0; i < c.NReduce; i++ {
					c.IdleTask[i] = i
				}
				c.Status = ScheduleReduce
			}
		}

	case TaskReduce:
		if c.Status == ScheduleReduce {
			delete(c.ProcessingTask, req.Index)
			if len(c.IdleTask) == 0 && len(c.ProcessingTask) == 0 {
				c.Status = ScheduleDone
			}
		}

	default:
		return fmt.Errorf("unknown task type: %d", req.Status)
	}

	return nil
}

// server start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	if err := rpc.Register(c); err != nil {
		panic(fmt.Errorf("rpc register err: %w", err))
	}
	rpc.HandleHTTP()
	sockName := coordinatorSock()
	_ = os.Remove(sockName)
	l, err := net.Listen("unix", sockName)
	if err != nil {
		panic(fmt.Errorf("listen err: %w", err))
	}
	go http.Serve(l, nil)
}

// closeWait is the time that the coordinator waits for all workers to exit.
const closeWait = 10 * time.Second

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	res := false

	if c.Status == ScheduleDone {
		res = true
		time.Sleep(closeWait)
	}

	return res
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		taskID2FileName: make(map[int]string),
		FileName:        files,
		IdleTask:        make([]int, len(files)),
		ProcessingTask:  make(map[int]time.Time),
		Status:          ScheduleMap,
		NReduce:         nReduce,
		NMap:            len(files),
	}

	c.server()
	return &c
}
