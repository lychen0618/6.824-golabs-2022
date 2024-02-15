package kvraft

import (
	"fmt"
	"log"
	"sync"
)

const (
	OK             = "OK"
	WaitComplete   = "WaitComplete"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutdateRPC  = "ErrOutDateRPC"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
}

func (args PutAppendArgs) String() string {
	return fmt.Sprintf("{key=%v, value=%v, op=%v, client_id=%v, cmd_id=%v}", args.Key, args.Value, args.Op, args.ClientId, args.CommandId)
}

type PutAppendReply struct {
	Err Err
}

func (reply PutAppendReply) String() string {
	return fmt.Sprintf("{err=%v}", reply.Err)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int64
}

func (args GetArgs) String() string {
	return fmt.Sprintf("{key=%v, client_id=%v, cmd_id=%v}", args.Key, args.ClientId, args.CommandId)
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply GetReply) String() string {
	return fmt.Sprintf("{err=%v, value=%v}", reply.Err, reply.Value)
}

type OpCache struct {
	mu        sync.Mutex
	cond      *sync.Cond
	clientId  int64
	commandId int64
	appliedId int64
	opType    OpType
	key       string
	value     string
	term      int
	index     int
	result    string
	err       Err
}

func (oc *OpCache) String() string {
	return fmt.Sprintf("{client_id=%v, cmd_id=%v, opType=%v, key=%v, value=%v, term=%v, index=%v, result=%v, err=%v}", oc.clientId, oc.commandId, oc.opType, oc.key, oc.value, oc.term, oc.index, oc.result, oc.err)
}

func NewOpCache(clientId int64, commandId int64, opType OpType, key string, value string, term int, index int) *OpCache {
	oc := OpCache{}
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.cond = sync.NewCond(&oc.mu)
	oc.clientId = clientId
	oc.commandId = commandId
	oc.opType = opType
	oc.key = key
	oc.value = value
	oc.term = term
	oc.index = index
	oc.err = WaitComplete
	oc.appliedId = 0
	return &oc
}

func (oc *OpCache) complete(result string, err Err) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.err != WaitComplete {
		log.Panicf("already finished: %v.", oc)
	}
	oc.result = result
	oc.err = err
	oc.cond.Broadcast()
}

func (oc *OpCache) ensureFinished() {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.err == WaitComplete {
		log.Panicf("not finished: %v.", oc)
	}
}

func (oc *OpCache) finished() bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	return oc.err != WaitComplete
}

func (oc *OpCache) completeIfUnfinished(result string, err Err) bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.err == WaitComplete {
		oc.result = result
		oc.err = err
		oc.cond.Broadcast()
		return true
	}
	return false
}

func (oc *OpCache) get() (string, Err) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	for oc.err == WaitComplete {
		oc.cond.Wait()
	}
	return oc.result, oc.err
}
