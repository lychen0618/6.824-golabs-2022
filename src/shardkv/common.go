package shardkv

import (
	"fmt"
	"sync"

	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                   = Err("OK")
	WaitComplete         = Err("WaitComplete")
	ErrNoKey             = Err("ErrNoKey")
	ErrWrongGroup        = Err("ErrWrongGroup")
	ErrWrongLeader       = Err("ErrWrongLeader")
	ErrOutdatedRPC       = Err("ErrOutdatedRPC")
	ErrNotAvailableYet   = Err("ErrNotAvailableYet")
	ErrShardNotExist     = Err("ErrShardNotExist")
	ErrConfigNumMismatch = Err("ErrConfigNumMismatch")
)

type Err string

type Identity struct {
	ClientId  int64
	CommandId int64
}

func (id Identity) String() string {
	return fmt.Sprintf("client_id=%v, cmd_id=%v", id.ClientId, id.CommandId)
}

type KeyValue struct {
	Key   string
	Value string
}

func (kv KeyValue) String() string {
	return fmt.Sprintf("key=%v, value=%v", kv.Key, kv.Value)
}

type KVState struct {
	ConfiguredNum     int
	KVStore           map[string]string
	LastExecutedOpMap map[int64]ExecutedOp
}

func (st KVState) String() string {
	return fmt.Sprintf("num=%v, kvStore=%v, lastExecutedOpMap=%v", st.ConfiguredNum, st.KVStore, st.LastExecutedOpMap)
}

type PartialConfiguration struct {
	Shards       []int
	ShardKVState []KVState
}

func (pc PartialConfiguration) String() string {
	return fmt.Sprintf("shards=%v, %v", pc.Shards, pc.ShardKVState)
}

type GetState struct {
	ConfigNum int
	Shards    []int
	Confirm   bool
}

func (gs GetState) String() string {
	return fmt.Sprintf("num=%v, shards=%v, confirm=%v", gs.ConfigNum, gs.Shards, gs.Confirm)
}

type ConfigState struct {
	Update           bool
	Completed        bool
	ConfiguredConfig shardctrler.Config
}

func (cs ConfigState) String() string {
	return fmt.Sprintf("update=%v, configuring=%v, configured_config=%v", cs.Update, cs.Completed, cs.ConfiguredConfig)
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Identity
	KeyValue
	Op string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

func (args PutAppendArgs) String() string {
	return fmt.Sprintf("{%v, %v, op=%v}", args.Identity, args.KeyValue, args.Op)
}

type PutAppendReply struct {
	Err Err
}

func (reply PutAppendReply) String() string {
	return fmt.Sprintf("{Err=%v}", reply.Err)
}

type GetArgs struct {
	Identity
	Key string
	// You'll have to add definitions here.
}

func (args GetArgs) String() string {
	return fmt.Sprintf("{%v, key=%v}", args.Identity, args.Key)
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply GetReply) String() string {
	return fmt.Sprintf("{Err=%v, Value=%v}", reply.Err, reply.Value)
}

type GetStateArgs struct {
	Identity
	GetState
}

func (args GetStateArgs) String() string {
	return fmt.Sprintf("{%v, %v}", args.Identity, args.GetState)
}

type GetStateReply struct {
	KVStateByShard map[int]KVState
	Err            Err
}

func (reply GetStateReply) String() string {
	return fmt.Sprintf("{%v, Err=%v}", reply.KVStateByShard, reply.Err)
}

type ReConfiguringArgs struct {
	Identity
	shardctrler.Config
}

func (args ReConfiguringArgs) String() string {
	return fmt.Sprintf("{%v, config=%v}", args.Identity, args.Config)
}

type ReConfiguringReply struct {
	ConfigState
	Err Err
}

func (reply ReConfiguringReply) String() string {
	return fmt.Sprintf("{config_state=%v, Err=%v}", reply.ConfigState, reply.Err)
}

type ReConfiguredArgs struct {
	Identity
	PartialConfiguration
}

func (args ReConfiguredArgs) String() string {
	return fmt.Sprintf("{%v, %v}", args.Identity, args.ShardKVState)
}

type ReConfiguredReply struct {
	MissingShards []int
	Err           Err
}

func (reply ReConfiguredReply) String() string {
	return fmt.Sprintf("{MissingShards=%v, Err=%v}", reply.MissingShards, reply.Err)
}

type ClientHandler struct {
	mu        sync.Mutex
	cond      *sync.Cond
	clientId  int64
	commandId int64
	result    interface{}
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

func (handler *ClientHandler) complete(result interface{}, err Err) {
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

func (handler *ClientHandler) completeIfUnfinished(result interface{}, err Err) bool {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.err == WaitComplete {
		handler.result, handler.err = result, err
		handler.cond.Broadcast()
		return true
	}
	return false
}

func (handler *ClientHandler) get() (interface{}, Err) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	for handler.err == WaitComplete {
		handler.cond.Wait()
	}
	return handler.result, handler.err
}
