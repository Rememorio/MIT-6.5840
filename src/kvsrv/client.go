package kvsrv

import (
	"crypto/rand"
	"math/big"

	"github.com/Rememorio/MIT-6.5840/labrpc"
)

type Clerk struct {
	server   *labrpc.ClientEnd
	clientId int64
	seqNum   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientId = nrand()
	ck.seqNum = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	reply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
	}
	ck.seqNum++
	reply := PutAppendReply{}
	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			return reply.Value
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
