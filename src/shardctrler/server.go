package shardctrler

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
	JOIN  OpType = "Join"
	LEAVE OpType = "Leave"
	MOVE  OpType = "Move"
	QUERY OpType = "Query"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	clients           map[int64]*ClientHandler
	lastExecutedOpMap map[int64]ExecutedOp
	lastAppliedIndex  int
	configs           []Config // indexed by config num
}

type Op struct {
	// Your data here.
	ClientId  int64
	CommandId int64
	Type      OpType
	Servers   map[int][]string // join
	GIDs      []int            // leave
	Shard     int              // move
	GID       int              // move
	Num       int              // query
}

func (op Op) String() string {
	return fmt.Sprintf("{client_id=%v, cmd_id=%v, type=%v, servers=%v, gids=%v, shard=%v, gid=%v, num=%v}", op.ClientId, op.CommandId, op.Type, op.Servers, op.GIDs, op.Shard, op.GID, op.Num)
}

type ExecutedOp struct {
	Op
	Result Config
}

func (ec ExecutedOp) String() string {
	return fmt.Sprintf("{op=%v, result=%v}", ec.Op, ec.Result)
}

func (sc *ShardCtrler) handleRequest(clientId int64, commandId int64, opType OpType, servers map[int][]string, gids []int, shard int, gid int, num int) (result Config, err Err) {
	DPrintf("S%d handle req, client_id=%v, cmd_id=%v, type=%s, servers=%v, gids=%v, shard=%v, gid=%v, num=%v\n", sc.me, clientId, commandId, opType, servers, gids, shard, gid, num)
	sc.mu.Lock()
	handler, exist := sc.clients[clientId]
	if !exist || commandId > handler.commandId {
		handler = NewClientHandler(clientId, commandId)
		sc.clients[clientId] = handler
	}
	if commandId == handler.commandId {
		lastExecutedOp, exist := sc.lastExecutedOpMap[clientId]
		if exist && lastExecutedOp.CommandId == commandId {
			// duplicate command, so fast return
			handler.complete(lastExecutedOp.Result, OK)
			sc.mu.Unlock()
			return handler.get()
		}
	}
	sc.mu.Unlock()
	if commandId < handler.commandId {
		log.Panicf("S%d receive invalid cmd_id(%v), handler%v.", sc.me, commandId, handler)
	}

	// start new round raft agreement
	op := Op{ClientId: clientId, CommandId: commandId, Type: opType, Servers: servers, GIDs: gids, Shard: shard, GID: gid, Num: num}
	_, currentTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		return Config{}, ErrWrongLeader
	}
	// start leadership checking
	go func(term int, op *Op) {
		for !sc.killed() && !handler.finished() {
			time.Sleep(time.Millisecond * 250)
			currentTerm, isLeader := sc.rf.GetState()
			sc.mu.Lock()
			DPrintf("S%d isLeader[%v] curTerm[%v] handler%v op%v\n", sc.me, isLeader, currentTerm, handler, op)
			if !isLeader || term != currentTerm {
				if handler.completeIfUnfinished(Config{}, ErrWrongLeader) {
					DPrintf("S%d timeout, handler%v op%v\n", sc.me, handler, op)
				}
				sc.mu.Unlock()
				return
			}
			sc.mu.Unlock()
		}
		DPrintf("S%d stop checking leader state, op%v\n", sc.me, op)
	}(currentTerm, &op)
	return handler.get()
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, err := sc.handleRequest(args.ClientId, args.CommandId, JOIN, args.Servers, nil, -1, -1, -1)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
	DPrintf("S%d finish join req, args%v, rsp%v\n", sc.me, args, reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, err := sc.handleRequest(args.ClientId, args.CommandId, LEAVE, nil, args.GIDs, -1, -1, -1)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
	DPrintf("S%d finish leave req, args%v, rsp%v\n", sc.me, args, reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, err := sc.handleRequest(args.ClientId, args.CommandId, MOVE, nil, nil, args.Shard, args.GID, -1)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
	DPrintf("S%d finish move req, args%v, rsp%v\n", sc.me, args, reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	config, err := sc.handleRequest(args.ClientId, args.CommandId, QUERY, nil, nil, -1, -1, args.Num)
	reply.Config, reply.Err = config, err
	reply.WrongLeader = (err == ErrWrongLeader)
	DPrintf("S%d finish query req, args%v, rsp%v\n", sc.me, args, reply)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) handleApplyMsg(msg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if msg.CommandValid {
		op := msg.Command.(Op)
		// update state machine
		var result Config
		lastExecutedOp := sc.lastExecutedOpMap[op.ClientId]
		if op.CommandId != lastExecutedOp.CommandId && op.CommandId != lastExecutedOp.CommandId+1 {
			log.Panicf("S%d receive msg with invalid command id. msg=%v, lastExecutedOpCmdId=%v\n", sc.me, msg, lastExecutedOp.CommandId)
		}
		if op.CommandId == lastExecutedOp.CommandId+1 {
			cf := sc.configs[len(sc.configs)-1]
			if op.Type == JOIN {
				servers := make(map[int][]string)
				for gid, group := range cf.Groups {
					servers[gid] = group
				}
				gil := shardToGroupItemList(cf.Shards, nil)
				for gid, group := range op.Servers {
					element, present := servers[gid]
					if present {
						log.Panicf("S%v: duplicate join, server already exist: servers[%v]=%v.", sc.me, gid, element)
					}
					gil = append(gil, GroupItem{gid, []int{}})
					servers[gid] = group
				}
				gil = reBalance(gil)
				shardsBalanced, _ := groupItemListToShard(gil)
				cf = Config{
					Num:    cf.Num + 1,
					Shards: shardsBalanced,
					Groups: servers,
				}
				sc.configs = append(sc.configs, cf)
			} else if op.Type == LEAVE {
				servers := make(map[int][]string)
				for gid, group := range cf.Groups {
					servers[gid] = group
				}
				for _, gid := range op.GIDs {
					if _, present := servers[gid]; !present {
						log.Panicf("S%v: leave non-exist server %v: servers=%v.", sc.me, gid, servers)
					}
					delete(servers, gid)
				}
				gidList := make([]int, len(servers))
				for gid := range servers {
					gidList = append(gidList, gid)
				}
				gil := shardToGroupItemList(cf.Shards, gidList)
				gil = reBalance(gil)
				shardsBalanced, _ := groupItemListToShard(gil)
				cf = Config{
					Num:    cf.Num + 1,
					Shards: shardsBalanced,
					Groups: servers,
				}
				sc.configs = append(sc.configs, cf)
			} else if op.Type == MOVE {
				servers := make(map[int][]string)
				for gid, group := range cf.Groups {
					servers[gid] = group
				}
				shards := cf.Shards
				shards[op.Shard] = op.GID
				cf = Config{
					Num:    cf.Num + 1,
					Shards: shards,
					Groups: servers,
				}
				sc.configs = append(sc.configs, cf)
			} else {
				// The Query RPC's argument is a configuration number.
				// The shardctrler replies with the configuration that has that number.
				// If the number is -1 or bigger than the biggest known configuration number,
				// the shardctrler should reply with the latest configuration.
				// The result of Query(-1) should reflect every Join, Leave,
				// or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
				if op.Num == -1 || op.Num >= len(sc.configs) {
					result = cf
					if op.Num >= len(sc.configs) {
						DPrintf("S%v: receive get config num %v, but not hold. configs=%v\n", sc.me, op.Num, sc.configs)
					}
				} else {
					result = sc.configs[op.Num]
				}
			}
			sc.lastExecutedOpMap[op.ClientId] = ExecutedOp{Op: op, Result: result}
			DPrintf("S%d apply op[%d], op%v\n", sc.me, msg.CommandIndex, op)
		} else {
			result = lastExecutedOp.Result
		}

		handler, exist := sc.clients[op.ClientId]
		if exist && handler.commandId == op.CommandId {
			handler.complete(result, OK)
		}
		if msg.CommandIndex <= sc.lastAppliedIndex {
			log.Panicf("S%d received invalid apply msg, raft_cmd_id=%v, last_applied_id=%v", sc.me, msg.CommandIndex, sc.lastAppliedIndex)
		}
		sc.lastAppliedIndex = msg.CommandIndex
	} else {
		log.Panicf("S%d received invalid apply msg%v\n", sc.me, msg)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clients = make(map[int64]*ClientHandler)
	sc.lastExecutedOpMap = make(map[int64]ExecutedOp)
	sc.lastAppliedIndex = 0

	go func() {
		for !sc.killed() {
			msg := <-sc.applyCh
			DPrintf("S%d: handle apply msg: %v\n", sc.me, msg)
			sc.handleApplyMsg(msg)
		}
	}()

	return sc
}
