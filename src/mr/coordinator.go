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

const taskTimeout = 10 * time.Second

type taskState int

const (
	idle taskState = iota
	inProgress
	completed
)

type taskInfo struct {
	state     taskState
	startTime time.Time
}

type phase int

const (
	mapPhase phase = iota
	reducePhase
	donePhase
)

// Coordinator manages MapReduce tasks.
type Coordinator struct {
	mu          sync.Mutex
	files       []string
	nReduce     int
	nMap        int
	phase       phase
	mapTasks    []taskInfo
	reduceTasks []taskInfo
}

// GetTask assigns a task to a requesting worker.
func (c *Coordinator) GetTask(_ *TaskRequest, reply *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case mapPhase:
		// Check for timed-out tasks
		c.recoverTimedOut(c.mapTasks)
		// Find an idle map task
		for i, t := range c.mapTasks {
			if t.state == idle {
				c.mapTasks[i].state = inProgress
				c.mapTasks[i].startTime = time.Now()
				reply.TaskType = MapTask
				reply.TaskId = i
				reply.FileName = c.files[i]
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}
		// All map tasks are in progress or completed; wait
		reply.TaskType = WaitTask
		return nil

	case reducePhase:
		c.recoverTimedOut(c.reduceTasks)
		for i, t := range c.reduceTasks {
			if t.state == idle {
				c.reduceTasks[i].state = inProgress
				c.reduceTasks[i].startTime = time.Now()
				reply.TaskType = ReduceTask
				reply.TaskId = i
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}
		reply.TaskType = WaitTask
		return nil

	case donePhase:
		reply.TaskType = ExitTask
		return nil
	}

	return nil
}

// TaskDone is called by a worker when it finishes a task.
func (c *Coordinator) TaskDone(req *DoneRequest, _ *DoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch req.TaskType {
	case MapTask:
		if c.phase == mapPhase && c.mapTasks[req.TaskId].state == inProgress {
			c.mapTasks[req.TaskId].state = completed
			if c.allDone(c.mapTasks) {
				c.phase = reducePhase
			}
		}
	case ReduceTask:
		if c.phase == reducePhase && c.reduceTasks[req.TaskId].state == inProgress {
			c.reduceTasks[req.TaskId].state = completed
			if c.allDone(c.reduceTasks) {
				c.phase = donePhase
			}
		}
	}

	return nil
}

// recoverTimedOut marks timed-out in-progress tasks as idle.
func (c *Coordinator) recoverTimedOut(tasks []taskInfo) {
	for i, t := range tasks {
		if t.state == inProgress && time.Since(t.startTime) > taskTimeout {
			tasks[i].state = idle
		}
	}
}

// allDone returns true if all tasks are completed.
func (c *Coordinator) allDone(tasks []taskInfo) bool {
	for _, t := range tasks {
		if t.state != completed {
			return false
		}
	}
	return true
}

// server starts the coordinator RPC server.
func (c *Coordinator) server() {
	if err := rpc.Register(c); err != nil {
		panic(fmt.Errorf("rpc register: %w", err))
	}
	rpc.HandleHTTP()
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, err := net.Listen("unix", sockName)
	if err != nil {
		panic(fmt.Errorf("listen: %w", err))
	}
	go http.Serve(l, nil)
}

// Done returns true when the entire MapReduce job is finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == donePhase
}

// MakeCoordinator creates a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		phase:       mapPhase,
		mapTasks:    make([]taskInfo, len(files)),
		reduceTasks: make([]taskInfo, nReduce),
	}
	c.server()
	return c
}
