package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// Client identity for duplicate detection
	ClientId int64
	SeqNum   int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
