package mr

import (
	"cmp"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"slices"
	"time"
)

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// hashMask is a mask for the lower 31 bits of a 32-bit integer.
const (
	hashMask    uint32 = 0x7fffffff
	taskWaiting        = 3 * time.Second
)

// iHash use iHash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func iHash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & hashMask)
}

// Worker main/mrworker.go calls this function.
func Worker(mapFunc func(string, string) []KeyValue,
	reduceFunc func(string, []string) string) {

	for {
		task, err := getMasterTask()
		if err != nil {
			log.Printf("get master task err: %v", err)
			continue
		}
		switch task.Status {
		case TaskMap:
			if err := workerMap(mapFunc, task); err != nil {
				log.Printf("worker map err: %v", err)
			}
		case TaskReduce:
			if err = workerReduce(reduceFunc, task); err != nil {
				log.Printf("worker reduce err: %v", err)
			}
		case TaskWaiting:
			time.Sleep(taskWaiting)
		case TaskDone:
			return
		default:
			log.Printf("unknown task status %v", task.Status)
		}
	}
}

// call send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, req any, rsp any) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		return fmt.Errorf("dialing err: %w", err)
	}
	defer c.Close()

	if err := c.Call(rpcName, req, rsp); err != nil {
		return fmt.Errorf("call err: %w", err)
	}
	return nil
}

// getMasterTask Send the RPC request, wait for the reply.
func getMasterTask() (*TaskResponse, error) {
	req, rsp := &TaskRequest{}, &TaskResponse{}
	if err := call("Coordinator.GetTask", req, rsp); err != nil {
		return nil, err
	}
	return rsp, nil
}

// workerMap handles the map task.
func workerMap(mapFunc func(string, string) []KeyValue, task *TaskResponse) error {
	file, err := os.Open(task.FileName)
	if err != nil {
		return fmt.Errorf("open %s err: %w", task.FileName, err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("read %s err: %w", task.FileName, err)
	}
	_ = file.Close()

	intermediate := mapFunc(task.FileName, string(content))
	tempFiles := make([]*os.File, task.NReduce)
	tempEncoders := make([]*json.Encoder, task.NReduce)

	for i, f := range tempFiles {
		f, err = os.CreateTemp("", "mr-tmp-*")
		if err != nil {
			return fmt.Errorf("create temp file [%d] err %w", i, err)
		}
		tempEncoders[i] = json.NewEncoder(f)
	}

	for i, kv := range intermediate {
		idx := iHash(kv.Key) % task.NReduce
		if err := tempEncoders[idx].Encode(&kv); err != nil {
			return fmt.Errorf("encode kv [%d] err: %w", i, err)
		}
	}

	for i, f := range tempFiles {
		if err := os.Rename(f.Name(), fmt.Sprintf("mr-%d-%d", task.Index, i)); err != nil {
			return fmt.Errorf("os rename %s err: %w", f.Name(), err)
		}
	}

	if err := workDoneCall(&WorkDoneRequest{Status: task.Status, Index: task.Index}); err != nil {
		return fmt.Errorf("worker done call err: %w", err)
	}
	return nil
}

// workDoneCall is to inform the master that the worker is done.
func workDoneCall(task *WorkDoneRequest) error {
	if err := call("Coordinator.WorkDone", task, &WorkDoneResponse{}); err != nil {
		return fmt.Errorf("worker done call err: %w", err)
	}
	return nil
}

// workerReduce handles the reduce task.
func workerReduce(reduceFunc func(string, []string) string, task *TaskResponse) error {
	var intermediate []KeyValue

	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.Index)
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("open %s err: %w", filename, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	slices.SortFunc(intermediate, func(a, b KeyValue) int { return cmp.Compare(a.Key, b.Key) })
	// Merge adjacent key-value pairs with the same key and save them to a file named `mr-out-i`.
	fileName := fmt.Sprintf("mr-out-%d", task.Index)
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("create %s err: %w", fileName, err)
	}
	defer func() { _ = file.Close() }()

	// Call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	n := len(intermediate)
	for i, j := 0, 0; i < n; i = j {
		for j = i + 1; j < n && intermediate[j].Key == intermediate[i].Key; j++ {
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFunc(intermediate[i].Key, values)

		// This is the correct format for each line of Reduce output.
		_, _ = fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
	}

	if err := workDoneCall(&WorkDoneRequest{Status: task.Status, Index: task.Index}); err != nil {
		return fmt.Errorf("worker done call err: %w", err)
	}
	return nil
}
