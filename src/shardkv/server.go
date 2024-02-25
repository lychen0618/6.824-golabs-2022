package shardkv

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
	"6.824/shardctrler"
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
	GET           OpType = "Get"
	PUT           OpType = "Put"
	APPEND        OpType = "Append"
	RECONFIGURING OpType = "RECONFIGURING"
	RECONFIGURED  OpType = "RECONFIGURED"
	GET_STATE     OpType = "GET_STATE"
)

// ExtraArgs actual type list
// KeyValue: GET, PUT, APPEND
// shardctrler.Config: RECONFIGURING
// KVState: RECONFIGURED
// [shardctrler.NShards]int: GET_STATE
type ExtraArgs interface{}

// ExtraReply actual type list
// string: GET
// shardctrler.Config: RECONFIGURING
// KVState: GET_STATE
type ExtraReply interface{}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Identity
	Type OpType
	Args ExtraArgs
}

func (op Op) String() string {
	return fmt.Sprintf("{%v, type=%v, args={%v}}", op.Identity, op.Type, op.Args)
}

type ExecutedOp struct {
	Op
	Result ExtraReply
}

func (ec ExecutedOp) String() string {
	return fmt.Sprintf("{op=%v, result=%v}", ec.Op, ec.Result)
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead      int32
	persister *raft.Persister
	mck       *shardctrler.Clerk
	clients   map[int64]*ClientHandler

	lastAppliedIndex int
	// persistent state start with Capital
	PreConfig       shardctrler.Config
	CurrConfig      shardctrler.Config
	CurrState       map[int]KVState
	AffectShards    [shardctrler.NShards]bool
	PreDeleteShards [shardctrler.NShards]bool
}

func (kv *ShardKV) handleRequest(identity Identity, opType OpType, extraArgs ExtraArgs) (ExtraReply, Err) {
	DPrintf("G%d-S%d handle req, %v, type=%s, %v\n", kv.gid, kv.me, identity, opType, extraArgs)
	kv.mu.Lock()
	clientId, commandId := identity.ClientId, identity.CommandId
	handler, exist := kv.clients[clientId]
	if exist && commandId < handler.commandId {
		kv.mu.Unlock()
		switch opType {
		case GET, PUT, APPEND, GET_STATE:
			// clerk will only send one request at a time
			log.Panicf("G%d-S%d: request will not complete, %v, opType=%v, %v, handler=%v\n", kv.gid, kv.me, identity, opType, extraArgs, handler)
		case RECONFIGURED, RECONFIGURING:
			DPrintf("G%d-S%d: configure request will not complete, %v, opType=%v, %v, handler=%v\n", kv.gid, kv.me, identity, opType, extraArgs, handler)
		}
		return nil, ErrOutdatedRPC
	}
	if !exist || commandId > handler.commandId {
		handler = NewClientHandler(clientId, commandId)
		kv.clients[clientId] = handler
	}
	kv.mu.Unlock()

	// start new round raft agreement
	op := Op{Identity: identity, Type: opType, Args: extraArgs}
	_, currentTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, ErrWrongLeader
	}
	// start leadership checking
	go func(term int, op *Op) {
		for !kv.killed() && !handler.finished() {
			time.Sleep(time.Millisecond * 250)
			currentTerm, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			DPrintf("G%d-S%d isLeader[%v] curTerm[%v] handler%v op%v\n", kv.gid, kv.me, isLeader, currentTerm, handler, op)
			if !isLeader || term != currentTerm {
				if handler.completeIfUnfinished("", ErrWrongLeader) {
					DPrintf("G%d-S%d timeout, handler%v op%v\n", kv.gid, kv.me, handler, op)
				}
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
		}
		DPrintf("G%d-S%d stop checking leader state, op%v\n", kv.gid, kv.me, op)
	}(currentTerm, &op)
	return handler.get()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	result, err := kv.handleRequest(args.Identity, GET, KeyValue{Key: args.Key, Value: "VALUE_GET"})
	reply.Err = err
	if err == OK {
		keyValue := result.(KeyValue)
		reply.Value = keyValue.Value
	}
	DPrintf("G%d-S%d: Get, args%v, rsp%v\n", kv.gid, kv.me, args, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, err := kv.handleRequest(args.Identity, OpType(args.Op), args.KeyValue)
	reply.Err = err
	DPrintf("G%d-S%d: PutAppend, args%v, rsp%v\n", kv.gid, kv.me, args, reply)
}

func (kv *ShardKV) GetState(args *GetStateArgs, reply *GetStateReply) {
	// Your code here.
	result, err := kv.handleRequest(args.Identity, GET_STATE, args.GetState)
	reply.Err = err
	if err == OK {
		reply.KVStateByShard = result.(map[int]KVState)
	}
	DPrintf("G%d-S%d: GetState, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)
}

func (kv *ShardKV) ReConfiguring(args *ReConfiguringArgs, reply *ReConfiguringReply) {
	result, err := kv.handleRequest(args.Identity, RECONFIGURING, args.Config)
	reply.Err = err
	if err == OK {
		reply.ConfigState = result.(ConfigState)
	}
	DPrintf("G%d-S%d: ReConfiguring, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)

}

func (kv *ShardKV) ReConfigured(args *ReConfiguredArgs, reply *ReConfiguredReply) {
	missingShards, err := kv.handleRequest(args.Identity, RECONFIGURED, args.PartialConfiguration)
	reply.Err = err
	if err == OK {
		reply.MissingShards = missingShards.([]int)
	}
	DPrintf("G%d-S%d: ReConfigured, args=%v, reply=%v.\n", kv.gid, kv.me, args, reply)
}

func (kv *ShardKV) handleApplyMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if msg.CommandValid {
		op := msg.Command.(Op)
		// update state machine
		var result ExtraReply
		switch op.Type {
		case GET, PUT, APPEND:
			keyValue := op.Args.(KeyValue)
			keyShard := key2shard(keyValue.Key)
			if kv.CurrConfig.Num != kv.PreConfig.Num && kv.AffectShards[keyShard] {
				result = ErrNotAvailableYet
				DPrintf("G%d-S%d: current configuring, curr=%v, pre=%v, affected=%v, shard=%v, unable to handle op: %v.\n", kv.gid, kv.me, kv.CurrConfig, kv.PreConfig, kv.AffectShards, keyShard, op)
			} else if configGid := kv.PreConfig.Shards[keyShard]; configGid != kv.gid {
				result = ErrWrongGroup
				DPrintf("G%d-S%d: wrong group, unable to handle op: %v, keyShard=%v, configGid=%v.\n", kv.gid, kv.me, op, keyShard, configGid)
			} else {
				shardState, exist := kv.CurrState[keyShard]
				if !exist || kv.PreDeleteShards[keyShard]{
					result = ErrShardNotExist
					DPrintf("G%d-S%d receive msg=%v, but state of shard=%v not exist or predeleted\n", kv.gid, kv.me, msg, keyShard)
				} else {
					lastExecutedOp := shardState.LastExecutedOpMap[op.ClientId]
					if op.CommandId < lastExecutedOp.CommandId {
						log.Panicf("G%d-S%d receive msg with invalid command id. msg=%v, lastExecutedOpCmdId=%v\n", kv.gid, kv.me, msg, lastExecutedOp.CommandId)
					}
					if op.CommandId > lastExecutedOp.CommandId {
						switch op.Type {
						case GET:
							result = KeyValue{"Key_GET", shardState.KVStore[keyValue.Key]}
						case PUT:
							shardState.KVStore[keyValue.Key] = keyValue.Value
						case APPEND:
							shardState.KVStore[keyValue.Key] = shardState.KVStore[keyValue.Key] + keyValue.Value
						}
						// only successfully command need to deduplicate
						shardState.LastExecutedOpMap[op.ClientId] = ExecutedOp{Op: op, Result: result}
					} else {
						result = lastExecutedOp.Result
					}
				}
			}
		case GET_STATE:
			getState := op.Args.(GetState)
			checkFlag := true
			for _, shard := range getState.Shards {
				state, present := kv.CurrState[shard]
				if !present || state.ConfiguredNum != getState.ConfigNum {
					checkFlag = false
					result = ErrShardNotExist
					if present {
						result = ErrConfigNumMismatch
					}
					break
				}
			}
			if !checkFlag {
				DPrintf("G%d-S%d: invalid get state op, getState=%v, currState=%v\n", kv.gid, kv.me, getState, kv.CurrState)
			} else {
				result = make(map[int]KVState)
				if !getState.Confirm {
					getKVStateByShard := make(map[int]KVState)
					for _, shard := range getState.Shards {
						kv.PreDeleteShards[shard] = true
						state := kv.CurrState[shard]
						getStateKVStore := make(map[string]string)
						getStateLastExecutedOpMap := make(map[int64]ExecutedOp)
						for key, value := range state.KVStore {
							getStateKVStore[key] = value
						}
						for cid, eop := range state.LastExecutedOpMap {
							getStateLastExecutedOpMap[cid] = eop
						}
						getKVStateByShard[shard] = KVState{
							ConfiguredNum:     state.ConfiguredNum,
							KVStore:           getStateKVStore,
							LastExecutedOpMap: getStateLastExecutedOpMap,
						}
					}
					result = getKVStateByShard
				} else {
					for _, shard := range getState.Shards {
						kv.PreDeleteShards[shard] = false
						delete(kv.CurrState, shard)
						DPrintf("G%d-S%d: delete state shard %v, currState=%v.\n", kv.gid, kv.me, shard, kv.CurrState)
					}
				}
			}
		case RECONFIGURING:
			newConfig := op.Args.(shardctrler.Config)
			DPrintf("G%d-S%d: reconfiguring request, op=%v, currConfig=%v, preConfig=%v.\n", kv.gid, kv.me, op, kv.CurrConfig, kv.PreConfig)
			if kv.PreConfig.Num == kv.CurrConfig.Num && newConfig.Num == kv.CurrConfig.Num+1 {
				result = ConfigState{true, false, kv.CurrConfig}
				for shard := 0; shard < shardctrler.NShards; shard++ {
					lastGid := kv.CurrConfig.Shards[shard]
					currGid := newConfig.Shards[shard]
					kv.AffectShards[shard] = false
					if lastGid != kv.gid && currGid == kv.gid {
						kv.AffectShards[shard] = true
						DPrintf("G%d-S%d: gain ownership of shard %v, currConfig=%v, gid=%v.\n", kv.gid, kv.me, shard, kv.CurrConfig, lastGid)
					} else if lastGid == kv.gid && currGid != kv.gid {
						shardState, present := kv.CurrState[shard]
						if !present {
							DPrintf("G%d-S%d: state of shard %v not exist, op=%v, shardState={%v}, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, shard, op, shardState, kv.PreConfig, kv.CurrConfig)
						}
						DPrintf("G%d-S%d: lost ownership of shard %v, currConfig=%v, make map, new_gid=%v\n", kv.gid, kv.me, shard, kv.CurrConfig, currGid)
					} else if lastGid == kv.gid && currGid == kv.gid {
						newKVState := kv.CurrState[shard]
						newKVState.ConfiguredNum = newConfig.Num
						kv.CurrState[shard] = newKVState
					}
				}
				kv.PreConfig = newConfig
				DPrintf("G%d-S%d: update PreConfig.Num to %v.\n", kv.gid, kv.me, kv.PreConfig.Num)
			} else {
				if kv.PreConfig.Num != kv.CurrConfig.Num {
					DPrintf("G%d-S%d: already in configuring state, op=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, op, kv.PreConfig, kv.CurrConfig)
				} else if kv.CurrConfig.Num == newConfig.Num {
					DPrintf("G%d-S%d: configuring already submitted, op=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, op, kv.PreConfig, kv.CurrConfig)
				} else if kv.CurrConfig.Num > newConfig.Num {
					DPrintf("G%d-S%d: configuring is outdated, op=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, op, kv.PreConfig, kv.CurrConfig)
				} else if newConfig.Num > kv.CurrConfig.Num+1 {
					DPrintf("G%d-S%d: configuring is too new, op=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, op, kv.PreConfig, kv.CurrConfig)
				} else {
					log.Panicf("G%d-S%d: unknown status, may be a bug, op=%v, pre_config=%v, curr_config=%v.\n", kv.gid, kv.me, op, kv.PreConfig, kv.CurrConfig)
				}
				result = ConfigState{false, kv.PreConfig.Num == kv.CurrConfig.Num, kv.CurrConfig}
			}
		case RECONFIGURED:
			partialState := op.Args.(PartialConfiguration)
			configNum := partialState.ShardKVState[0].ConfiguredNum
			result = []int{}
			if kv.CurrConfig.Num == kv.PreConfig.Num && kv.CurrConfig.Num == configNum {
				DPrintf("G%d-S%d: configured accept(duplicated). partialState={%v}, curr_config=%v, pre_config=%v.\n", kv.gid, kv.me, partialState, kv.CurrConfig, kv.PreConfig)
			} else if kv.CurrConfig.Num == kv.PreConfig.Num && kv.CurrConfig.Num != configNum {
				DPrintf("G%d-S%d: current is not configuring status. partialState={%v}, curr_config=%v, pre_config=%v.\n", kv.gid, kv.me, partialState, kv.CurrConfig, kv.PreConfig)
				result = ErrConfigNumMismatch
			} else if kv.CurrConfig.Num != configNum {
				DPrintf("G%d-S%d: configured not matched. partialState={%v}, curr_config=%v, pre_config=%v.\n", kv.gid, kv.me, partialState, kv.CurrConfig, kv.PreConfig)
				result = ErrConfigNumMismatch
			} else {
				var duplicated bool
				if len(partialState.Shards) > 0 {
					fetched := kv.AffectShards[partialState.Shards[0]]
					for _, shard := range partialState.Shards {
						if kv.AffectShards[shard] != fetched {
							log.Panicf("G%d-S%d: inconsistent configured state. partialState={%v}, curr_config=%v, pre_config=%v, affected=%v.\n", kv.gid, kv.me, partialState, kv.CurrConfig, kv.PreConfig, kv.AffectShards)
						}
						kv.AffectShards[shard] = false
					}
					duplicated = !fetched
				} else {
					duplicated = false
				}

				var missingShards []int
				for shard, isAffected := range kv.AffectShards {
					if isAffected {
						missingShards = append(missingShards, shard)
					}
				}
				if duplicated {
					DPrintf("G%d-S%d: receive duplicated partialState, already updated. partialState={%v}, curr_config=%v, pre_config=%v, affected_shards=%v.\n", kv.gid, kv.me, partialState, kv.CurrConfig, kv.PreConfig, kv.AffectShards)
				} else {
					for idx, shard := range partialState.Shards {
						shardState := partialState.ShardKVState[idx]
						clonedKVStore := make(map[string]string)
						for key, value := range shardState.KVStore {
							clonedKVStore[key] = value
						}
						clonedLastExecutedOpMap := make(map[int64]ExecutedOp)
						for cid, eOp := range shardState.LastExecutedOpMap {
							clonedLastExecutedOpMap[cid] = eOp
						}
						kv.CurrState[shard] = KVState{
							ConfiguredNum:     shardState.ConfiguredNum + 1,
							KVStore:           clonedKVStore,
							LastExecutedOpMap: clonedLastExecutedOpMap,
						}
					}
					if len(missingShards) == 0 {
						kv.CurrConfig = kv.PreConfig
						DPrintf("G%d-S%d: missingShards is empty, update ConfiguredConfig.Num to %v.\n", kv.gid, kv.me, kv.CurrConfig.Num)
					} else {
						DPrintf("G%d-S%d: still wait for state, missingShards=%v, partialState=%v.\n", kv.gid, kv.me, missingShards, partialState)
					}
				}
				result = missingShards
			}
		}

		handler, exist := kv.clients[op.ClientId]
		if exist && handler.commandId == op.CommandId {
			if err, containsErr := result.(Err); !containsErr {
				handler.complete(result, OK)
			} else {
				handler.complete(result, err)
			}
		}
		kv.lastAppliedIndex = msg.CommandIndex

		if kv.maxraftstate > 0 {
			raftStateSize := kv.persister.RaftStateSize()
			if raftStateSize > kv.maxraftstate {
				DPrintf("G%d-S%d: raft state size greater maxraftstate(%v > %v), trim log.\n", kv.gid, kv.me, raftStateSize, kv.maxraftstate)
				snapshot := kv.encodeState(kv.PreConfig, kv.CurrConfig, kv.CurrState, kv.AffectShards, kv.PreDeleteShards)
				kv.rf.Snapshot(kv.lastAppliedIndex, snapshot)
			}
		}
	} else if msg.SnapshotValid {
		// receive snapshot indicate is not leader
		kv.PreConfig, kv.CurrConfig, kv.CurrState, kv.AffectShards, kv.PreDeleteShards = kv.decodeState(msg.Snapshot)
		for shard, gid := range kv.PreConfig.Shards {
			if gid == kv.gid && !kv.AffectShards[shard] {
				for _, op := range kv.CurrState[shard].LastExecutedOpMap {
					handler, exist := kv.clients[op.ClientId]
					if exist && handler.commandId == op.CommandId {
						handler.complete(op.Result, OK)
					}
				}
			}
		}
	} else {
		DPrintf("G%d-S%d received invalid apply msg%v, maybe due to applyCh close\n", kv.gid, kv.me, msg)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) encodeState(preConfig shardctrler.Config, currConfig shardctrler.Config, currState map[int]KVState, affectShards [shardctrler.NShards]bool, preDeleteShards [shardctrler.NShards]bool) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(preConfig); err != nil {
		log.Panicf("G%d-S%d: fail to encode preConfig: %v, err=%v.", kv.gid, kv.me, preConfig, err)
	}
	if err := e.Encode(currConfig); err != nil {
		log.Panicf("G%d-S%d: fail to encode currConfig: %v, err=%v.", kv.gid, kv.me, currConfig, err)
	}
	if err := e.Encode(currState); err != nil {
		log.Panicf("G%d-S%d: fail to encode currState: %v, err=%v.", kv.gid, kv.me, currState, err)
	}
	if err := e.Encode(affectShards); err != nil {
		log.Panicf("G%d-S%d: fail to encode affectShards: %v, err=%v.", kv.gid, kv.me, affectShards, err)
	}
	if err := e.Encode(preDeleteShards); err != nil {
		log.Panicf("G%d-S%d: fail to encode preDeleteShards: %v, err=%v.", kv.gid, kv.me, preDeleteShards, err)
	}
	return w.Bytes()
}

func (kv *ShardKV) decodeState(data []byte) (preConfig shardctrler.Config, currConfig shardctrler.Config, currState map[int]KVState, affectShards [shardctrler.NShards]bool, preDeleteShards [shardctrler.NShards]bool) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&preConfig); err != nil {
		log.Panicf("G%d-S%d: fail to decode preConfig, Err=%v", kv.gid, kv.me, err)
	}
	if err := d.Decode(&currConfig); err != nil {
		log.Panicf("G%d-S%d: fail to decode currConfig, Err=%v", kv.gid, kv.me, err)
	}
	if err := d.Decode(&currState); err != nil {
		log.Panicf("G%d-S%d: fail to decode currState, Err=%v", kv.gid, kv.me, err)
	}
	if err := d.Decode(&affectShards); err != nil {
		log.Panicf("G%d-S%d: fail to decode affectShards, Err=%v", kv.gid, kv.me, err)
	}
	if err := d.Decode(&preDeleteShards); err != nil {
		log.Panicf("G%d-S%d: fail to decode preDeleteShards, Err=%v", kv.gid, kv.me, err)
	}
	return
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(KVState{})
	labgob.Register(PartialConfiguration{})
	labgob.Register(KeyValue{})
	labgob.Register(GetState{})
	labgob.Register(ConfigState{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.clients = make(map[int64]*ClientHandler)
	kv.persister = persister
	if persister.SnapshotSize() != 0 {
		kv.PreConfig, kv.CurrConfig, kv.CurrState, kv.AffectShards, kv.PreDeleteShards = kv.decodeState(persister.ReadSnapshot())
	} else {
		kv.PreConfig = shardctrler.Config{}
		kv.CurrConfig = shardctrler.Config{}
		kv.CurrState = map[int]KVState{}
	}

	go func() {
		for !kv.killed() {
			msg := <-kv.applyCh
			DPrintf("G%d-S%d: handle apply msg: %v\n", kv.gid, kv.me, msg)
			kv.handleApplyMsg(msg)
		}
	}()

	go func() {
		cck := MakeConfigureClerk(kv)
		DPrintf("G%d-S%d: onPollConfiguration start\n", kv.gid, kv.me)
		for !kv.killed() {
			t1 := time.Now().UnixMilli()
			cck.onPollConfiguration()
			t2 := time.Now().UnixMilli()
			time.Sleep(time.Duration(100-(t2-t1)) * time.Millisecond)
		}
	}()

	return kv
}
