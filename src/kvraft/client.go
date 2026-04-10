package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"github.com/Rememorio/MIT-6.5840/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	clientId int64
	seqNum   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.seqNum = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqNum:   atomic.AddInt64(&ck.seqNum, 1),
	}
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != Err(ErrWrongLeader) {
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqNum:   atomic.AddInt64(&ck.seqNum, 1),
	}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != Err(ErrWrongLeader) {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
