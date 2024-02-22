package shardctrler

import (
	"fmt"
	"sync"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	WaitComplete   = "WaitComplete"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type JoinArgs struct {
	ClientId  int64
	CommandId int64
	Servers   map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClientId  int64
	CommandId int64
	GIDs      []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClientId  int64
	CommandId int64
	Shard     int
	GID       int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	ClientId  int64
	CommandId int64
	Num       int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type ClientHandler struct {
	mu        sync.Mutex
	cond      *sync.Cond
	clientId  int64
	commandId int64
	result    Config
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

func (handler *ClientHandler) complete(result Config, err Err) {
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

func (handler *ClientHandler) completeIfUnfinished(result Config, err Err) bool {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.err == WaitComplete {
		handler.result, handler.err = result, err
		handler.cond.Broadcast()
		return true
	}
	return false
}

func (handler *ClientHandler) get() (Config, Err) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	for handler.err == WaitComplete {
		handler.cond.Wait()
	}
	return handler.result, handler.err
}
