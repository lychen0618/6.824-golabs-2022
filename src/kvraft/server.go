package kvraft

import (
	"bytes"
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

type ExecutedOp struct {
	Op
	Result string
}

func (ec ExecutedOp) String() string {
	return fmt.Sprintf("{op=%v, result=%v}", ec.Op, ec.Result)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	kvStore           map[string]string
	clients           map[int64]*ClientHandler
	lastExecutedOpMap map[int64]ExecutedOp
	lastAppliedIndex  int
}

func (kv *KVServer) handleRequest(clientId int64, commandId int64, opType OpType, key string, value string) (result string, err Err) {
	DPrintf("S%d handle req, client_id=%v, cmd_id=%v, type=%s, key=%s, value=%s\n", kv.me, clientId, commandId, opType, key, value)
	kv.mu.Lock()
	handler, exist := kv.clients[clientId]
	if !exist || commandId > handler.commandId {
		handler = NewClientHandler(clientId, commandId)
		kv.clients[clientId] = handler
	}
	if commandId == handler.commandId {
		lastExecutedOp, exist := kv.lastExecutedOpMap[clientId]
		if exist && lastExecutedOp.CommandId == commandId {
			// duplicate command, so fast return
			handler.complete(lastExecutedOp.Result, OK)
			kv.mu.Unlock()
			return handler.get()
		}
	}
	kv.mu.Unlock()
	if commandId < handler.commandId {
		log.Panicf("S%d receive invalid cmd_id(%v), handler%v.", kv.me, commandId, handler)
	}

	// start new round raft agreement
	op := Op{ClientId: clientId, CommandId: commandId, Type: opType, Key: key, Value: value}
	_, currentTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	}
	// start leadership checking
	go func(term int, op *Op) {
		for !kv.killed() && !handler.finished() {
			time.Sleep(time.Millisecond * 250)
			currentTerm, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			DPrintf("S%d isLeader[%v] curTerm[%v] handler%v op%v\n", kv.me, isLeader, currentTerm, handler, op)
			if !isLeader || term != currentTerm {
				if handler.completeIfUnfinished("", ErrWrongLeader) {
					DPrintf("S%d timeout, handler%v op%v\n", kv.me, handler, op)
				}
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
		}
		DPrintf("S%d stop checking leader state, op%v\n", kv.me, op)
	}(currentTerm, &op)
	return handler.get()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	result, err := kv.handleRequest(args.ClientId, args.CommandId, GET, args.Key, "VALUE_GET")
	reply.Value = result
	reply.Err = err
	DPrintf("S%d finish req, args%v, rsp%v\n", kv.me, args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, err := kv.handleRequest(args.ClientId, args.CommandId, OpType(args.Op), args.Key, args.Value)
	reply.Err = err
	DPrintf("S%d finish req, args%v, rsp%v\n", kv.me, args, reply)
}

func (kv *KVServer) handleApplyMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if msg.CommandValid {
		op := msg.Command.(Op)
		// update state machine
		result := ""
		lastExecutedOp := kv.lastExecutedOpMap[op.ClientId]
		if op.CommandId != lastExecutedOp.CommandId && op.CommandId != lastExecutedOp.CommandId+1 {
			log.Panicf("S%d receive msg with invalid command id. msg=%v, lastExecutedOpCmdId=%v\n", kv.me, msg, lastExecutedOp.CommandId)
		}
		if op.CommandId == lastExecutedOp.CommandId+1 {
			if op.Type == PUT {
				kv.kvStore[op.Key] = op.Value
			} else if op.Type == APPEND {
				kv.kvStore[op.Key] += op.Value
			} else {
				result = kv.kvStore[op.Key]
			}
			kv.lastExecutedOpMap[op.ClientId] = ExecutedOp{Op: op, Result: result}
			DPrintf("S%d apply op[%d], op%v\n", kv.me, msg.CommandIndex, op)
		} else {
			result = lastExecutedOp.Result
		}

		handler, exist := kv.clients[op.ClientId]
		if exist && handler.commandId == op.CommandId {
			handler.complete(result, OK)
		}
		if msg.CommandIndex <= kv.lastAppliedIndex {
			log.Panicf("S%d received invalid apply msg, raft_cmd_id=%v, last_applied_id=%v", kv.me, msg.CommandIndex, kv.lastAppliedIndex)
		}
		kv.lastAppliedIndex = msg.CommandIndex
	} else if msg.SnapshotValid {
		// receive snapshot indicate is not leader
		if msg.SnapshotIndex < kv.lastAppliedIndex {
			log.Panicf("S%d received invalid snapshot msg, snap_shot_id=%v, last_applied_id=%v", kv.me, msg.SnapshotIndex, kv.lastAppliedIndex)
		}
		kv.kvStore, kv.lastAppliedIndex, kv.lastExecutedOpMap = kv.decodeState(msg.Snapshot)
		for _, op := range kv.lastExecutedOpMap {
			handler, exist := kv.clients[op.ClientId]
			if exist && handler.commandId == op.CommandId {
				handler.complete(op.Result, OK)
			}
		}
		DPrintf("S%d: update lastExecutedOpMap: %v\n", kv.me, kv.lastExecutedOpMap)
	} else {
		log.Panicf("S%d received invalid apply msg%v\n", kv.me, msg)
	}

	if kv.maxraftstate > 0 {
		raftStateSize := kv.persister.RaftStateSize()
		if raftStateSize > kv.maxraftstate {
			DPrintf("S%d: raft state size greater maxraftstate(%v > %v), trim log.\n", kv.me, raftStateSize, kv.maxraftstate)
			snapshot := kv.encodeState(kv.kvStore, kv.lastAppliedIndex, kv.lastExecutedOpMap)
			kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
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

func (kv *KVServer) encodeState(kvStore map[string]string, lastAppliedIndex int, lastExecutedOpMap map[int64]ExecutedOp) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kvStore); err != nil {
		log.Panicf("S%d fail to encode kvStore: %v", kv.me, kvStore)
	}
	if err := e.Encode(lastAppliedIndex); err != nil {
		log.Panicf("S%d fail to encode lastAppliedIndex: %v", kv.me, lastAppliedIndex)
	}
	if err := e.Encode(lastExecutedOpMap); err != nil {
		log.Panicf("S%d fail to encode lastExecutedOpMap: %v", kv.me, lastExecutedOpMap)
	}
	return w.Bytes()
}

func (kv *KVServer) decodeState(data []byte) (kvStore map[string]string, lastAppliedIndex int, lastExecutedOpMap map[int64]ExecutedOp) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&kvStore); err != nil {
		log.Panicf("S%d fail to decode kvStore: %v", kv.me, kvStore)
	}
	if err := d.Decode(&lastAppliedIndex); err != nil {
		log.Panicf("S%d fail to decode lastAppliedIndex: %v", kv.me, lastAppliedIndex)
	}
	if err := d.Decode(&lastExecutedOpMap); err != nil {
		log.Panicf("S%d fail to decode lastExecutedOpMap: %v", kv.me, lastExecutedOpMap)
	}
	return
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
	kv.clients = make(map[int64]*ClientHandler)
	kv.lastExecutedOpMap = make(map[int64]ExecutedOp)
	kv.persister = persister
	if persister.SnapshotSize() != 0 {
		kv.kvStore, kv.lastAppliedIndex, kv.lastExecutedOpMap = kv.decodeState(persister.ReadSnapshot())
	}

	go func() {
		for !kv.killed() {
			msg := <-kv.applyCh
			DPrintf("S%d: handle apply msg: %v\n", kv.me, msg)
			kv.handleApplyMsg(msg)
		}
	}()

	return kv
}
