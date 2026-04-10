package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rememorio/MIT-6.5840/labgob"
	"github.com/Rememorio/MIT-6.5840/labrpc"
	"github.com/Rememorio/MIT-6.5840/raft"
	"github.com/Rememorio/MIT-6.5840/shardctrler"
)

type Op struct {
	Type     string // "Get", "Put", "Append", "Reconfig", "InstallShard"
	Key      string
	Value    string
	ClientId int64
	SeqNum   int64
	// For Reconfig
	Config shardctrler.Config
	// For InstallShard
	Shard     int
	ShardData map[string]string
	ShardSeq  map[int64]int64
	ConfigNum int
}

type result struct {
	Err   Err
	Value string
}

// Track shard states
const (
	Serving   = 0
	Pulling   = 1 // Need to pull data from another group
	BePulling = 2 // Another group is pulling data from us
	GCing     = 3
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int
	persister    *raft.Persister
	dead         int32

	mck *shardctrler.Clerk

	// Per-shard data
	shardData []map[string]string
	// Per-shard duplicate detection
	shardSeq []map[int64]int64

	// Current config
	config     shardctrler.Config
	prevConfig shardctrler.Config

	// Track whether each shard needs migration
	// shardReady[shard] = true means we have proper data for this shard
	shardReady [shardctrler.NShards]bool

	// Waiting channels
	waitChs map[int]chan result
}

func (kv *ShardKV) canServe(shard int) bool {
	return kv.config.Shards[shard] == kv.gid && kv.shardReady[shard]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

func (kv *ShardKV) submitOp(op Op) result {
	kv.mu.Lock()
	shard := key2shard(op.Key)
	if !kv.canServe(shard) {
		kv.mu.Unlock()
		return result{Err: ErrWrongGroup}
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return result{Err: ErrWrongLeader}
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
		return result{Err: ErrWrongLeader}
	}
}

// Migrate RPC - called by other groups to get shard data
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Only return data if we've already applied the config that gave away this shard
	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Data = make(map[string]string)
	reply.LastSeq = make(map[int64]int64)

	for k, v := range kv.shardData[args.Shard] {
		reply.Data[k] = v
	}
	for k, v := range kv.shardSeq[args.Shard] {
		reply.LastSeq[k] = v
	}

	reply.Err = OK
}

func (kv *ShardKV) applier() {
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

		switch op.Type {
		case "Get", "Put", "Append":
			shard := key2shard(op.Key)
			if !kv.canServe(shard) {
				res.Err = ErrWrongGroup
			} else {
				isDuplicate := false
				if op.Type != "Get" {
					if lastSeq, ok := kv.shardSeq[shard][op.ClientId]; ok && lastSeq >= op.SeqNum {
						isDuplicate = true
					}
				}

				if !isDuplicate {
					switch op.Type {
					case "Get":
						res.Value = kv.shardData[shard][op.Key]
					case "Put":
						kv.shardData[shard][op.Key] = op.Value
						kv.shardSeq[shard][op.ClientId] = op.SeqNum
					case "Append":
						kv.shardData[shard][op.Key] += op.Value
						kv.shardSeq[shard][op.ClientId] = op.SeqNum
					}
				}
				res.Err = OK
			}

		case "Reconfig":
			if op.Config.Num == kv.config.Num+1 {
				kv.prevConfig = kv.config
				// Mark shards that need pulling as not ready
				for shard := 0; shard < shardctrler.NShards; shard++ {
					if op.Config.Shards[shard] == kv.gid {
						if kv.config.Shards[shard] == kv.gid {
							// We already own this shard, keep it ready
							// shardReady[shard] stays true
						} else if kv.config.Shards[shard] == 0 || kv.config.Num == 0 {
							// Shard was unassigned or this is the first config
							kv.shardReady[shard] = true
						} else {
							// Need to pull data from another group
							kv.shardReady[shard] = false
						}
					} else {
						// We don't own this shard
						kv.shardReady[shard] = false
					}
				}
				kv.config = op.Config
			}

		case "InstallShard":
			if op.ConfigNum == kv.config.Num && kv.config.Shards[op.Shard] == kv.gid && !kv.shardReady[op.Shard] {
				kv.shardData[op.Shard] = make(map[string]string)
				for k, v := range op.ShardData {
					kv.shardData[op.Shard][k] = v
				}
				for k, v := range op.ShardSeq {
					if v > kv.shardSeq[op.Shard][k] {
						kv.shardSeq[op.Shard][k] = v
					}
				}
				kv.shardReady[op.Shard] = true
			}
		}

		// Notify waiting RPC handler
		if ch, ok := kv.waitChs[msg.CommandIndex]; ok {
			_, isLeader := kv.rf.GetState()
			if isLeader {
				ch <- res
			}
			delete(kv.waitChs, msg.CommandIndex)
		}

		// Snapshot if needed
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.takeSnapshot(msg.CommandIndex)
		}

		kv.mu.Unlock()
	}
}

// Periodically poll the shard controller for new configs
func (kv *ShardKV) pollConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			currentNum := kv.config.Num
			// Only advance if all shards for current config are ready
			allReady := true
			for shard := 0; shard < shardctrler.NShards; shard++ {
				if kv.config.Shards[shard] == kv.gid && !kv.shardReady[shard] {
					allReady = false
					break
				}
			}
			kv.mu.Unlock()

			if allReady {
				newConfig := kv.mck.Query(currentNum + 1)
				if newConfig.Num == currentNum+1 {
					kv.rf.Start(Op{
						Type:   "Reconfig",
						Config: newConfig,
					})
				}
			}
		}
		time.Sleep(80 * time.Millisecond)
	}
}

// Periodically try to pull shard data that we need
func (kv *ShardKV) pullShards() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			config := kv.config
			prevConfig := kv.prevConfig
			var shardsToPull []int
			for shard := 0; shard < shardctrler.NShards; shard++ {
				if config.Shards[shard] == kv.gid && !kv.shardReady[shard] {
					shardsToPull = append(shardsToPull, shard)
				}
			}
			kv.mu.Unlock()

			for _, shard := range shardsToPull {
				go kv.pullShard(shard, prevConfig, config.Num)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) pullShard(shard int, prevConfig shardctrler.Config, configNum int) {
	oldGid := prevConfig.Shards[shard]
	servers, ok := prevConfig.Groups[oldGid]
	if !ok {
		return
	}

	args := MigrateArgs{
		Shard:     shard,
		ConfigNum: prevConfig.Num,
	}

	for _, srv := range servers {
		end := kv.make_end(srv)
		var reply MigrateReply
		ok := end.Call("ShardKV.Migrate", &args, &reply)
		if ok && reply.Err == OK {
			kv.rf.Start(Op{
				Type:      "InstallShard",
				Shard:     shard,
				ShardData: reply.Data,
				ShardSeq:  reply.LastSeq,
				ConfigNum: configNum,
			})
			return
		}
	}
}

func (kv *ShardKV) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shardData)
	e.Encode(kv.shardSeq)
	e.Encode(kv.config)
	e.Encode(kv.prevConfig)
	e.Encode(kv.shardReady)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardData []map[string]string
	var shardSeq []map[int64]int64
	var config shardctrler.Config
	var prevConfig shardctrler.Config
	var shardReady [shardctrler.NShards]bool

	if d.Decode(&shardData) != nil ||
		d.Decode(&shardSeq) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&prevConfig) != nil ||
		d.Decode(&shardReady) != nil {
		return
	}
	kv.shardData = shardData
	kv.shardSeq = shardSeq
	kv.config = config
	kv.prevConfig = prevConfig
	kv.shardReady = shardReady
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.shardData[i] == nil {
			kv.shardData[i] = make(map[string]string)
		}
		if kv.shardSeq[i] == nil {
			kv.shardSeq[i] = make(map[int64]int64)
		}
	}
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitChs = make(map[int]chan result)

	kv.shardData = make([]map[string]string, shardctrler.NShards)
	kv.shardSeq = make([]map[int64]int64, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardData[i] = make(map[string]string)
		kv.shardSeq[i] = make(map[int64]int64)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.applySnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.pollConfig()
	go kv.pullShards()

	return kv
}
