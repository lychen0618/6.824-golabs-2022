package kvraft

import (
	"fmt"
	"sync"
)

const (
	OK             = "OK"
	WaitComplete   = "WaitComplete"
	ErrWrongLeader = "ErrWrongLeader"
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

type ClientHandler struct {
	mu        sync.Mutex
	cond      *sync.Cond
	clientId  int64
	commandId int64
	result    string
	err       Err
}

func (c *ClientHandler) String() string {
	return fmt.Sprintf("{client_id=%v, cmd_id=%v, result=%v, err=%v}", c.clientId, c.commandId, c.result, c.err)
}

func NewClientHandler(clientId int64, commandId int64) *ClientHandler {
	handler := ClientHandler{
		clientId:  clientId,
		commandId: commandId,
		err:       WaitComplete,
	}
	handler.cond = sync.NewCond(&handler.mu)
	return &handler
}

func (handler *ClientHandler) complete(result string, err Err) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.result, handler.err = result, err
	handler.cond.Broadcast()
}

func (handler *ClientHandler) finished() bool {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return handler.err != WaitComplete
}

func (handler *ClientHandler) completeIfUnfinished(result string, err Err) bool {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.err == WaitComplete {
		handler.result, handler.err = result, err
		handler.cond.Broadcast()
		return true
	}
	return false
}

func (handler *ClientHandler) get() (string, Err) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	for handler.err == WaitComplete {
		handler.cond.Wait()
	}
	return handler.result, handler.err
}
