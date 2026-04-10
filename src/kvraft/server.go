package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rememorio/MIT-6.5840/labgob"
	"github.com/Rememorio/MIT-6.5840/labrpc"
	"github.com/Rememorio/MIT-6.5840/raft"
)

type Op struct {
	Type     string // "Get", "Put", "Append"
	Key      string
	Value    string
	ClientId int64
	SeqNum   int64
}

type result struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int
	persister    *raft.Persister

	// State machine
	data map[string]string

	// Duplicate detection: clientId -> last completed seqNum and result
	lastSeq map[int64]int64

	// Waiting channels: log index -> chan result
	waitChs map[int]chan result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	res := kv.submitOp(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	res := kv.submitOp(op)
	reply.Err = res.Err
}

func (kv *KVServer) submitOp(op Op) result {
	kv.mu.Lock()

	// Check duplicate for Put/Append
	if op.Type != "Get" {
		if lastSeq, ok := kv.lastSeq[op.ClientId]; ok && lastSeq >= op.SeqNum {
			kv.mu.Unlock()
			return result{Err: OK}
		}
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return result{Err: Err(ErrWrongLeader)}
	}

	kv.mu.Lock()
	ch := make(chan result, 1)
	kv.waitChs[index] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		return res
	case <-time.After(2 * time.Second):
		kv.mu.Lock()
		delete(kv.waitChs, index)
		kv.mu.Unlock()
		return result{Err: Err(ErrWrongLeader)}
	}
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if msg.SnapshotValid {
			kv.mu.Lock()
			kv.applySnapshot(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}

		if !msg.CommandValid {
			continue
		}

		op := msg.Command.(Op)

		kv.mu.Lock()

		var res result

		// Check duplicate for Put/Append
		isDuplicate := false
		if op.Type != "Get" {
			if lastSeq, ok := kv.lastSeq[op.ClientId]; ok && lastSeq >= op.SeqNum {
				isDuplicate = true
			}
		}

		if !isDuplicate {
			switch op.Type {
			case "Get":
				res.Value = kv.data[op.Key]
				res.Err = OK
			case "Put":
				kv.data[op.Key] = op.Value
				res.Err = OK
				kv.lastSeq[op.ClientId] = op.SeqNum
			case "Append":
				kv.data[op.Key] += op.Value
				res.Err = OK
				kv.lastSeq[op.ClientId] = op.SeqNum
			}
		} else {
			res.Err = OK
		}

		// Notify waiting RPC handler
		if ch, ok := kv.waitChs[msg.CommandIndex]; ok {
			// Only notify if the op matches (same term/leader)
			_, isLeader := kv.rf.GetState()
			if isLeader {
				ch <- res
			}
			delete(kv.waitChs, msg.CommandIndex)
		}

		// Check if we need to snapshot
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.takeSnapshot(msg.CommandIndex)
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastSeq)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var data map[string]string
	var lastSeq map[int64]int64

	if d.Decode(&data) != nil || d.Decode(&lastSeq) != nil {
		return
	}
	kv.data = data
	kv.lastSeq = lastSeq
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.data = make(map[string]string)
	kv.lastSeq = make(map[int64]int64)
	kv.waitChs = make(map[int]chan result)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Restore from snapshot
	kv.applySnapshot(persister.ReadSnapshot())

	go kv.applier()

	return kv
}
