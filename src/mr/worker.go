package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue is emitted by Map functions.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey implements sort.Interface for []KeyValue based on Key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// ihash returns a hash for use in partitioning keys to reduce tasks.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is the main loop for a MapReduce worker.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply, err := callGetTask()
		if err != nil {
			// Coordinator is probably gone; exit.
			return
		}

		switch reply.TaskType {
		case MapTask:
			doMap(mapf, reply)
		case ReduceTask:
			doReduce(reducef, reply)
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			return
		}
	}
}

func callGetTask() (*TaskResponse, error) {
	req := &TaskRequest{}
	reply := &TaskResponse{}
	if err := call("Coordinator.GetTask", req, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func callTaskDone(taskType TaskType, taskId int) {
	req := &DoneRequest{TaskType: taskType, TaskId: taskId}
	reply := &DoneResponse{}
	call("Coordinator.TaskDone", req, reply)
}

func doMap(mapf func(string, string) []KeyValue, task *TaskResponse) {
	// Read input file
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Printf("cannot open %v: %v", task.FileName, err)
		return
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		log.Printf("cannot read %v: %v", task.FileName, err)
		return
	}

	// Call map function
	kva := mapf(task.FileName, string(content))

	// Partition into nReduce intermediate files
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// Write each bucket to a temp file, then atomically rename
	for i, bucket := range buckets {
		tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Printf("cannot create temp file: %v", err)
			return
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range bucket {
			if err := enc.Encode(&kv); err != nil {
				log.Printf("cannot encode kv: %v", err)
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return
			}
		}
		tmpFile.Close()

		outName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		if err := os.Rename(tmpFile.Name(), outName); err != nil {
			log.Printf("cannot rename %v to %v: %v", tmpFile.Name(), outName, err)
			return
		}
	}

	callTaskDone(MapTask, task.TaskId)
}

func doReduce(reducef func(string, []string) string, task *TaskResponse) {
	// Read all intermediate files for this reduce task
	var intermediate []KeyValue
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("cannot open %v: %v", filename, err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Sort by key
	sort.Sort(ByKey(intermediate))

	// Write output to a temp file, then atomically rename
	tmpFile, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		log.Printf("cannot create temp file: %v", err)
		return
	}

	// Call Reduce on each distinct key
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpFile.Close()

	outName := fmt.Sprintf("mr-out-%d", task.TaskId)
	if err := os.Rename(tmpFile.Name(), outName); err != nil {
		log.Printf("cannot rename %v to %v: %v", tmpFile.Name(), outName, err)
		return
	}

	callTaskDone(ReduceTask, task.TaskId)
}

// call sends an RPC to the coordinator.
func call(rpcname string, args any, reply any) error {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Call(rpcname, args, reply)
}
