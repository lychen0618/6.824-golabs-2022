package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastKnownLeader int
	clientId        int64
	commandId       int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastKnownLeader = 0
	ck.clientId = nrand()
	ck.commandId = 0
	return ck
}

func (ck *Clerk) sendKVGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) sendKVPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.commandId += 1
	args := GetArgs{Key: key, ClientId: ck.clientId, CommandId: ck.commandId}
	for {
		si := ck.lastKnownLeader
		for i := 0; i < len(ck.servers); i++ {
			reply := GetReply{}
			ok := ck.sendKVGet(si, &args, &reply)
			if ok && reply.Err == OK {
				ck.lastKnownLeader = si
				return reply.Value
			}
			si = (si + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.commandId += 1
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, CommandId: ck.commandId}
	for {
		si := ck.lastKnownLeader
		for i := 0; i < len(ck.servers); i++ {
			reply := PutAppendReply{}
			ok := ck.sendKVPutAppend(si, &args, &reply)
			if ok && reply.Err == OK {
				ck.lastKnownLeader = si
				return
			}
			si = (si + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
