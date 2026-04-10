package shardctrler

import (
	"sort"
	"sync"
	"time"

	"github.com/Rememorio/MIT-6.5840/labgob"
	"github.com/Rememorio/MIT-6.5840/labrpc"
	"github.com/Rememorio/MIT-6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num
	lastSeq map[int64]int64
	waitChs map[int]chan opResult
}

type Op struct {
	Type     string           // "Join", "Leave", "Move", "Query"
	Servers  map[int][]string // for Join
	GIDs     []int            // for Leave
	Shard    int              // for Move
	GID      int              // for Move
	Num      int              // for Query
	ClientId int64
	SeqNum   int64
}

type opResult struct {
	Config Config
	Err    Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Type:     "Join",
		Servers:  args.Servers,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	res := sc.submitOp(op)
	if res.Err == "WrongLeader" {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Type:     "Leave",
		GIDs:     args.GIDs,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	res := sc.submitOp(op)
	if res.Err == "WrongLeader" {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Type:     "Move",
		Shard:    args.Shard,
		GID:      args.GID,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	res := sc.submitOp(op)
	if res.Err == "WrongLeader" {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Type:     "Query",
		Num:      args.Num,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	res := sc.submitOp(op)
	if res.Err == "WrongLeader" {
		reply.WrongLeader = true
	} else {
		reply.Config = res.Config
	}
}

func (sc *ShardCtrler) submitOp(op Op) opResult {
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return opResult{Err: "WrongLeader"}
	}

	sc.mu.Lock()
	ch := make(chan opResult, 1)
	sc.waitChs[index] = ch
	sc.mu.Unlock()

	select {
	case res := <-ch:
		return res
	case <-time.After(2 * time.Second):
		sc.mu.Lock()
		delete(sc.waitChs, index)
		sc.mu.Unlock()
		return opResult{Err: "WrongLeader"}
	}
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if !msg.CommandValid {
			continue
		}

		op := msg.Command.(Op)
		sc.mu.Lock()

		var res opResult

		// Check for duplicate (except Query which is idempotent)
		isDuplicate := false
		if op.Type != "Query" {
			if lastSeq, ok := sc.lastSeq[op.ClientId]; ok && lastSeq >= op.SeqNum {
				isDuplicate = true
			}
		}

		if !isDuplicate {
			switch op.Type {
			case "Join":
				sc.applyJoin(op.Servers)
				sc.lastSeq[op.ClientId] = op.SeqNum
			case "Leave":
				sc.applyLeave(op.GIDs)
				sc.lastSeq[op.ClientId] = op.SeqNum
			case "Move":
				sc.applyMove(op.Shard, op.GID)
				sc.lastSeq[op.ClientId] = op.SeqNum
			case "Query":
				// Query doesn't modify state
			}
		}

		// Build result
		if op.Type == "Query" {
			if op.Num == -1 || op.Num >= len(sc.configs) {
				res.Config = sc.configs[len(sc.configs)-1]
			} else {
				res.Config = sc.configs[op.Num]
			}
		}
		res.Err = OK

		if ch, ok := sc.waitChs[msg.CommandIndex]; ok {
			_, isLeader := sc.rf.GetState()
			if isLeader {
				ch <- res
			}
			delete(sc.waitChs, msg.CommandIndex)
		}

		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyJoin(servers map[int][]string) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}
	// Copy existing groups
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	// Add new groups
	for gid, servers := range servers {
		newConfig.Groups[gid] = servers
	}
	// Rebalance shards
	sc.rebalance(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyLeave(gids []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}
	// Copy groups except leaving ones
	leaving := make(map[int]bool)
	for _, gid := range gids {
		leaving[gid] = true
	}
	for gid, servers := range lastConfig.Groups {
		if !leaving[gid] {
			newConfig.Groups[gid] = servers
		}
	}
	// Clear shards from leaving groups
	for i, gid := range newConfig.Shards {
		if leaving[gid] {
			newConfig.Shards[i] = 0
		}
	}
	// Rebalance
	sc.rebalance(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyMove(shard int, gid int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) rebalance(config *Config) {
	nGroups := len(config.Groups)
	if nGroups == 0 {
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	// Get sorted list of GIDs for determinism
	gids := make([]int, 0, nGroups)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	avg := NShards / nGroups
	extra := NShards % nGroups

	// Target: first 'extra' groups get avg+1 shards, rest get avg
	target := make(map[int]int)
	for i, gid := range gids {
		if i < extra {
			target[gid] = avg + 1
		} else {
			target[gid] = avg
		}
	}

	// Count current shards per group
	counts := make(map[int]int)
	for _, gid := range config.Shards {
		if gid != 0 {
			counts[gid]++
		}
	}

	// Collect shards that need to be moved (from groups with too many, or unassigned)
	var freeShards []int
	for i, gid := range config.Shards {
		if gid == 0 {
			freeShards = append(freeShards, i)
		} else if _, ok := config.Groups[gid]; !ok {
			freeShards = append(freeShards, i)
			config.Shards[i] = 0
		} else if counts[gid] > target[gid] {
			freeShards = append(freeShards, i)
			config.Shards[i] = 0
			counts[gid]--
		}
	}

	// Assign free shards to groups that need more
	idx := 0
	for _, gid := range gids {
		for counts[gid] < target[gid] && idx < len(freeShards) {
			config.Shards[freeShards[idx]] = gid
			counts[gid]++
			idx++
		}
	}
}

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.lastSeq = make(map[int64]int64)
	sc.waitChs = make(map[int]chan opResult)

	go sc.applier()

	return sc
}
