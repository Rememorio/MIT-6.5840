package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	SeqNum   int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	SeqNum   int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Shard migration RPC
type MigrateArgs struct {
	Shard     int
	ConfigNum int
}

type MigrateReply struct {
	Err     Err
	Data    map[string]string
	LastSeq map[int64]int64
}
