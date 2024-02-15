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
	PUT           = "Put"
	APPEND        = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string
}

func (o Op) String() string {
	if o.Type == GET {
		return fmt.Sprintf("{type=%s, key=%s}", "GET", o.Key)
	} else {
		return fmt.Sprintf("{type=%s, key=%s, value=%s}", o.Type, o.Key, o.Value)
	}
}

type ApplyChEvent struct {
	index int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	waitingEvent map[int]chan ApplyChEvent
	kvStore      map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	event := make(chan ApplyChEvent)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Type: GET, Key: args.Key}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("S%d op[%d], req%v\n", kv.me, index, args)
	kv.waitingEvent[index] = event
	kv.mu.Unlock()
	<-event
	kv.mu.Lock()
	val, exist := kv.kvStore[args.Key]
	if exist {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	DPrintf("S%d op[%d], rsp%v\n", kv.me, index, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	event := make(chan ApplyChEvent)
	kv.mu.Lock()
	op := Op{Type: OpType(args.Op), Key: args.Key, Value: args.Value}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("S%d op[%d], req%v\n", kv.me, index, args)
	kv.waitingEvent[index] = event
	kv.mu.Unlock()
	<-event
	reply.Err = OK
	DPrintf("S%d op[%d], rsp%v\n", kv.me, index, reply)
}

func (kv *KVServer) applyChRecevier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			op, _ := msg.Command.(Op)
			kv.mu.Lock()
			event := kv.waitingEvent[msg.CommandIndex]
			if op.Type == PUT {
				kv.kvStore[op.Key] = op.Value
			} else if op.Type == APPEND {
				kv.kvStore[op.Key] += op.Value
			}
			DPrintf("S%d apply op[%d], op%v\n", kv.me, msg.CommandIndex, op)
			kv.mu.Unlock()
			event <- ApplyChEvent{msg.CommandIndex}
		default:
			time.Sleep(50 * time.Millisecond)
		}
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.waitingEvent = make(map[int]chan ApplyChEvent)
	kv.kvStore = make(map[string]string)

	go kv.applyChRecevier()

	return kv
}
