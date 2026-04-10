package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Record of a completed Append for duplicate detection.
// We only need to remember the last completed operation per client.
type LastReply struct {
	SeqNum int64
	Value  string // the old value returned by Append
}

type KVServer struct {
	mu sync.Mutex

	data      map[string]string
	lastReply map[int64]LastReply // clientId -> last completed append reply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check for duplicate
	if last, ok := kv.lastReply[args.ClientId]; ok && last.SeqNum == args.SeqNum {
		return
	}

	kv.data[args.Key] = args.Value
	// Record this operation (Put doesn't return a meaningful value but we track it for dedup)
	kv.lastReply[args.ClientId] = LastReply{SeqNum: args.SeqNum, Value: ""}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check for duplicate
	if last, ok := kv.lastReply[args.ClientId]; ok && last.SeqNum == args.SeqNum {
		reply.Value = last.Value
		return
	}

	oldValue := kv.data[args.Key]
	kv.data[args.Key] = oldValue + args.Value
	reply.Value = oldValue

	// Record this operation for duplicate detection
	kv.lastReply[args.ClientId] = LastReply{SeqNum: args.SeqNum, Value: oldValue}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.lastReply = make(map[int64]LastReply)
	return kv
}
