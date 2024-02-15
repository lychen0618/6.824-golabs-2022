package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	GET    OpType = "Get"
	PUT    OpType = "Put"
	APPEND OpType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
	Type      OpType
	Key       string
	Value     string
}

func (o Op) String() string {
	if o.Type == GET {
		return fmt.Sprintf("{client_id=%v, cmd_id=%v, type=%s, key=%s}", o.ClientId, o.CommandId, o.Type, o.Key)
	} else {
		return fmt.Sprintf("{client_id=%v, cmd_id=%v, type=%s, key=%s, value=%s}", o.ClientId, o.CommandId, o.Type, o.Key, o.Value)
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore     map[string]string
	clients     map[int64]*OpCache
	lastApplied map[int64]int64
}

func (kv *KVServer) createOpCacheWithLock(op Op, term int, index int) *OpCache {
	opCache := NewOpCache(op.ClientId, op.CommandId, op.Type, op.Key, op.Value, term, index)
	kv.clients[opCache.clientId] = opCache
	go func() {
		for !opCache.finished() {
			time.Sleep(time.Millisecond * 250)
			currentTerm, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			DPrintf("S%d isLeader[%v] curTerm[%v] opCache%v\n", kv.me, isLeader, currentTerm, opCache)
			if !isLeader || opCache.term != currentTerm {
				if opCache.completeIfUnfinished("", ErrWrongLeader) {
					DPrintf("S%d: %v timeout\n", kv.me, opCache)
				}
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
		}
		DPrintf("S%d opCache finish, opCache%v\n", kv.me, opCache)
	}()
	return opCache
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("S%d get req, args%v\n", kv.me, args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	opCache, exist := kv.clients[args.ClientId]
	if !exist || args.CommandId > opCache.commandId {
		// new command
		op := Op{ClientId: args.ClientId, CommandId: args.CommandId, Type: GET, Key: args.Key}
		index, currentTerm, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		opCache = kv.createOpCacheWithLock(op, currentTerm, index)
		kv.mu.Unlock()
		result, err := opCache.get()
		reply.Value, reply.Err = result, err
		kv.mu.Lock()
		if err != OK { // TODO
			delete(kv.clients, args.ClientId)
		}
	} else if args.CommandId == opCache.commandId {
		// duplicate command
		kv.mu.Unlock()
		result, err := opCache.get()
		reply.Value, reply.Err = result, err
		kv.mu.Lock()
		if err != OK {
			delete(kv.clients, args.ClientId)
		}
	} else {
		log.Panicf("S%v receive invalid id (opCache.id=%v). args=%v, opCache=%v.", kv.me, opCache.commandId, args, opCache)
	}
	DPrintf("S%d finish req, args%v, rsp%v\n", kv.me, args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("S%d get req, args%v\n", kv.me, args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	opCache, exist := kv.clients[args.ClientId]
	if !exist || args.CommandId > opCache.commandId {
		// new command
		op := Op{ClientId: args.ClientId, CommandId: args.CommandId, Type: OpType(args.Op), Key: args.Key, Value: args.Value}
		index, currentTerm, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		opCache = kv.createOpCacheWithLock(op, currentTerm, index)
		kv.mu.Unlock()
		_, err := opCache.get()
		reply.Err = err
		kv.mu.Lock()
		if err != OK {
			delete(kv.clients, args.ClientId)
		}
	} else if args.CommandId == opCache.commandId {
		// duplicate command
		kv.mu.Unlock()
		_, err := opCache.get()
		reply.Err = err
		kv.mu.Lock()
		if err != OK {
			delete(kv.clients, args.ClientId)
		}
	} else {
		log.Panicf("S%v receive invalid id (opCache.id=%v). args=%v, opCache=%v.", kv.me, opCache.commandId, args, opCache)
	}
	DPrintf("S%d finish req, args%v, rsp%v\n", kv.me, args, reply)
}

func (kv *KVServer) applyChRecevier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		DPrintf("S%d: Receive apply msg: %v\n", kv.me, msg)
		op := msg.Command.(Op)
		// update state machine
		result := ""
		lastApplied := kv.lastApplied[op.ClientId]
		if op.CommandId != lastApplied && op.CommandId != lastApplied+1 {
			log.Panicf("S%d receive msg with invalid command id. msg=%v, lastApplied=%v\n", kv.me, msg, lastApplied)
		}
		if op.CommandId == lastApplied+1 {
			if op.Type == PUT {
				kv.kvStore[op.Key] = op.Value
			} else if op.Type == APPEND {
				kv.kvStore[op.Key] += op.Value
			} else {
				result = kv.kvStore[op.Key]
			}
			DPrintf("S%d apply op[%d], op%v\n", kv.me, msg.CommandIndex, op)
		} else {
			if op.Type == GET {
				result = kv.kvStore[op.Key]
			}
		}
		kv.lastApplied[op.ClientId] = op.CommandId

		opCache, exist := kv.clients[op.ClientId]
		if !exist || op.CommandId > opCache.commandId {
			// new command
			if exist {
				opCache.ensureFinished()
			}
			opCache = kv.createOpCacheWithLock(op, -1, -1)
			opCache.complete(result, OK)
		} else if opCache.commandId == op.CommandId {
			opCache.completeIfUnfinished(result, OK)
		} else {
			DPrintf("S%d: receive outdated msg, just update state but not change opCache. msg=%v, opCache=%v.", kv.me, msg, opCache)
		}

		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.clients = make(map[int64]*OpCache)
	kv.lastApplied = make(map[int64]int64)

	go kv.applyChRecevier()

	return kv
}
