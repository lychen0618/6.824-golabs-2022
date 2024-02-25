package shardkv

import (
	"log"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

// ConfigureClerk
// poll configuration periodically from shardctrler and send `re-configuring` RPC
// get state from other group and send `re-configured` RPC
type ConfigureClerk struct {
	mck      *shardctrler.Clerk
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mainClientId     int64 // random id, should be unique globally
	mainCommandId    int64 // for a client, monotonically increase from 0
	mutipleClientId  [shardctrler.NShards]int64
	mutipleCommandId [shardctrler.NShards]int64
	kv               *ShardKV
	me               int
	gid              int
	preConfigNum     int
	configuredNum    int
}

func MakeConfigureClerk(kv *ShardKV) *ConfigureClerk {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ck := new(ConfigureClerk)
	ck.kv = kv
	ck.mck = shardctrler.MakeClerk(kv.ctrlers)
	ck.make_end = kv.make_end
	ck.mainClientId = nrand()
	for shard := range ck.mutipleClientId {
		ck.mutipleClientId[shard] = nrand()
	}
	ck.me = kv.me
	ck.gid = kv.gid
	ck.preConfigNum = 0
	ck.configuredNum = 0
	return ck
}

func (ck *ConfigureClerk) onPollConfiguration() {
	// !!Important: carefully check assumption, queryConfig2 may change quickly (concurrent re-configure)
	// !!Important: carefully reuse clientId, as we assume one time, one RPC per client
	queryConfig := ck.mck.Query(-1)
	//DPrintf("G%d-S%d: query config = %v.\n", ck.gid, ck.me, queryConfig)
	if queryConfig.Num < ck.preConfigNum {
		log.Panicf("G%d-S%d: queryConfig.Num < ck.conf.ConfiguredConfig.Num, something went wrong, queryConfig=%v, conf=%v\n", ck.gid, ck.me, queryConfig, ck.preConfigNum)
	}

	if queryConfig.Num == ck.preConfigNum && queryConfig.Num == ck.configuredNum {
		return
	}

	var alreadyCompleted bool
	var configuredConfig shardctrler.Config
	var preConfig shardctrler.Config
	var outConfigstate ConfigState
	for i := 0; ; i++ {
		DPrintf("G%d-S%d: sendReConfiguring round=%v, queryConfig=%v\n", ck.gid, ck.me, i, queryConfig)
		configState, err := ck.sendReConfiguring(ck.mainClientId, atomic.AddInt64(&ck.mainCommandId, 1), queryConfig)
		if err != OK {
			DPrintf("G%d-S%d: do Re-Configure fail, Err=%v.\n", ck.gid, ck.me, err)
			return
		}
		outConfigstate = configState
		configuredConfig = configState.ConfiguredConfig
		if configState.Update {
			DPrintf("G%d-S%d: update re-configuring, configNum=%v, configState=%v, queryConfig=%v.\n", ck.gid, ck.me, ck.preConfigNum, configState, queryConfig)
			preConfig = queryConfig
			alreadyCompleted = false
			break
		} else if !configState.Completed {
			preConfig = ck.mck.Query(configuredConfig.Num + 1)
			alreadyCompleted = false
			break
		} else {
			// not update, completed, should configure a new one
			if configuredConfig.Num == queryConfig.Num-1 {
				// queryConfig - 1 is completed, but update to queryConfig fail ???
				log.Panicf("G%d-S%d: during re-configuring, unknown situation #1, i=%v, queryConfig=%v, configState=%v, configuredConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, configuredConfig, alreadyCompleted)
			} else if configuredConfig.Num < queryConfig.Num-1 {
				queryConfig = ck.mck.Query(configuredConfig.Num + 1)
				DPrintf("G%d-S%d: query config is too new to update, retry with old one, i=%v, queryConfig=%v, configState=%v, configuredConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, configuredConfig, alreadyCompleted)
				continue
			} else if configuredConfig.Num == queryConfig.Num {
				// queryConfig is completed, other client has already done our job
				alreadyCompleted = true
				preConfig = queryConfig
				DPrintf("G%d-S%d: already submit and complete, i=%v, queryConfig=%v, configState=%v, configuredConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, configuredConfig, alreadyCompleted)
				break
			} else {
				queryConfig = ck.mck.Query(-1)
				DPrintf("G%d-S%d: concurrent configuration change, i=%v, queryConfig=%v, configState=%v, configuredConfig=%v, alreadyCompleted=%v.\n", ck.gid, ck.me, i, queryConfig, configState, configuredConfig, alreadyCompleted)
				continue
			}
		}
	}

	if preConfig.Num > ck.preConfigNum {
		ck.preConfigNum = preConfig.Num
		DPrintf("G%d-S%d: update last known preConfig.Num to %v.\n", ck.gid, ck.me, ck.preConfigNum)
	}

	if !alreadyCompleted {
		if configuredConfig.Num+1 != preConfig.Num {
			log.Panicf("G%d-S%d: during re-configured, something went wrong, preConfig=%v, configuredConfig=%v, queryConfig=%v, configState=%v.\n", ck.gid, ck.me, preConfig, configuredConfig, queryConfig, outConfigstate)
		}

		gid2shards := make(map[int][]int)
		for shard := range preConfig.Shards {
			configuredGid := configuredConfig.Shards[shard]
			preGid := preConfig.Shards[shard]
			if configuredGid != ck.gid && preGid == ck.gid {
				gid2shards[configuredGid] = append(gid2shards[configuredGid], shard)
				DPrintf("G%d-S%d: gain ownership of shard %v, configuredConfig=%v, gid=%v.\n", ck.gid, ck.me, shard, configuredConfig, configuredGid)
			} else if configuredGid == ck.gid && preGid != ck.gid {
				DPrintf("G%d-S%d: lost ownership of shard %v, configuredConfig=%v, new_gid=%v.\n", ck.gid, ck.me, configuredConfig, shard, preGid)
			}
		}
		if len(gid2shards) > 0 && (gid2shards[0] == nil) {
			ch := make(chan int)
			for gid, shards := range gid2shards {
				gid := gid
				shards := shards
				anyShard := shards[0]
				clientId := ck.mutipleClientId[anyShard]
				commandId := &ck.mutipleCommandId[anyShard]
				go func() {
					state, err := ck.sendGetState(clientId, atomic.AddInt64(commandId, 1), configuredConfig, gid, shards, false)
					normal := -1
					switch err {
					case OK:
						DPrintf("G%d-S%d: get state success, gid=%v, shards=%v.\n", ck.gid, ck.me, gid, shards)
						shardKVState := make([]KVState, 0)
						for _, shard := range shards {
							shardKVState = append(shardKVState, state[shard])
						}
						missing, err := ck.sendReConfigured(clientId, atomic.AddInt64(commandId, 1), PartialConfiguration{Shards: shards, ShardKVState: shardKVState})
						DPrintf("G%d-S%d: re-configured, shards=%v, missing=%v, err=%v.\n", ck.gid, ck.me, shards, missing, err)
						if err == OK {
							_, err = ck.sendGetState(clientId, atomic.AddInt64(commandId, 1), configuredConfig, gid, shards, true)
							DPrintf("G%d-S%d: get state confirm, err=%v.\n", ck.gid, ck.me, err)
							if err == OK {
								normal = 0
							}
						}
					}
					if normal == -1 {
						DPrintf("G%d-S%d: get and configured state failed, gid=%v, shards=%v, but get err=%v, state=%v\n", ck.gid, ck.me, gid, shards, err, state)
					}
					ch <- normal
				}()
			}

			cnt := 0
			for range gid2shards {
				missing := <-ch
				if missing == 0 {
					cnt++
				}
			}
			if cnt == len(gid2shards) {
				ck.configuredNum = preConfig.Num
				DPrintf("G%d-S%d: missing is empty, update last known configured Num to %v.\n", ck.gid, ck.me, ck.configuredNum)
			}
		} else {
			var partialConfig PartialConfiguration
			if len(gid2shards) > 0 {
				shards := gid2shards[0]
				kvStates := make([]KVState, 0)
				for i := 0; i < len(shards); i++ {
					kvStates = append(kvStates, KVState{configuredConfig.Num, map[string]string{}, map[int64]ExecutedOp{}})
				}
				partialConfig = PartialConfiguration{Shards: shards, ShardKVState: kvStates}
			} else {
				partialConfig = PartialConfiguration{Shards: nil, ShardKVState: []KVState{{configuredConfig.Num, nil, nil}}}
			}
			missing, err := ck.sendReConfigured(ck.mainClientId, atomic.AddInt64(&ck.mainCommandId, 1), partialConfig)
			if err != OK {
				DPrintf("G%d-S%d: expect re-configured success, but err=%v.\n", ck.gid, ck.me, err)
				return
			}
			if len(missing) > 0 {
				log.Panicf("G%d-S%d: expect missing is empty, but got: %v.\n", ck.gid, ck.me, missing)
			}
			ck.configuredNum = preConfig.Num
			DPrintf("G%d-S%d: group not affected, just update last known configured Num to %v.\n", ck.gid, ck.me, ck.configuredNum)
		}
	} else {
		ck.configuredNum = configuredConfig.Num
		DPrintf("G%d-S%d: no need to send re-configured, update last known configured Num to %v.\n", ck.gid, ck.me, ck.configuredNum)
	}

}

func (ck *ConfigureClerk) sendGetState(clientId int64, commandId int64, lastConfig shardctrler.Config, gid int, shards []int, confirm bool) (map[int]KVState, Err) {
	DPrintf("G%d-S%d: GetState. gid=%v, shards=%v.\n", ck.gid, ck.me, gid, shards)
	args := GetStateArgs{Identity{clientId, commandId}, GetState{lastConfig.Num, shards, confirm}}
	for {
		//gid := kv.ctrlerConfig.Shards[shard]
		if servers, ok := lastConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetStateReply
				ok := srv.Call("ShardKV.GetState", &args, &reply)
				DPrintf("G%d-S%d: call get finish, server='%v', args=%v, reply=%v, ok=%v.\n", ck.gid, ck.me, servers[si], args, reply, ok)
				if ok && (reply.Err == OK || reply.Err == ErrConfigNumMismatch || reply.Err == ErrShardNotExist) {
					// successfully get state
					DPrintf("G%d-S%d: finish get state, args=%v, reply=%v.", ck.gid, ck.me, args, reply)
					return reply.KVStateByShard, reply.Err
				}
				DPrintf("G%d-S%d: send get state receive %v, args=%v, reply=%v.\n", ck.gid, ck.me, reply.Err, args, reply)
				// ... not ok, or ErrWrongLeader
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *ConfigureClerk) sendReConfiguring(clientId int64, commandId int64, config shardctrler.Config) (configState ConfigState, err Err) {
	args := ReConfiguringArgs{
		Identity: Identity{clientId, commandId},
		Config:   config,
	}
	reply := ReConfiguringReply{}
	ck.kv.ReConfiguring(&args, &reply)
	return reply.ConfigState, reply.Err
}

func (ck *ConfigureClerk) sendReConfigured(clientId int64, commandId int64, partialConfig PartialConfiguration) (missingShards []int, err Err) {
	args := ReConfiguredArgs{
		Identity:             Identity{clientId, commandId},
		PartialConfiguration: partialConfig,
	}
	reply := ReConfiguredReply{}
	ck.kv.ReConfigured(&args, &reply)
	return reply.MissingShards, reply.Err
}
