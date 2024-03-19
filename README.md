### Lab4: Sharded Key/Value Service
* client + shardctrler + shardkv
* Two main components: 
    * shardkv: a set of replica groups. Each replica group is responsible for a subset of the shards. A replica consists of a handful of servers that use Raft to replicate the group's shards.
    * shardctrler: decides which replica group should serve each shard; this information is called the configuration.
* Clients: sends each RPC to the replica group responsible for the RPC's key. It re-tries if the replica group says it is not responsible for the key; in that case, the client code asks the shard controller for the latest configuration and tries again

* Within a single replica group, all group members must agree on when a reconfiguration occurs relative to client Put/Append/Get requests (reconfiguration occurred before or after the Put operation): use raft to log not only put but also reconfiguration.

* Shardkv periodically fetch the latest configuration from the shardctrler.

* **Shard migration: reconfiguring + getstate + reconfigured.**

* **Garbage collection of state & Client requests during configuration changes.**

### Lab3: Fault-tolerant Key/Value Service
* Linearizability: use lock to protect critical sections; store the result of last command for every client to avoid execute one command twice.
* One situation: a leader that has called Start() for a Clerk's RPC, but loses its leadership before the request is committed to the log.

    1. Server detect losing leadership by calling rf.GetState() periodically, then response clients.
    2. Clients resend request.
    3. Server avoid executing command twice. (client_id + command_id)

### Lab2: Raft

### Lab1: MapReduce
* A worker will exit if the coordinator has exit or it receives a task with TaskType 0 which represents this task is a pseudo-task.

* Reduce tasks will be issued when all map tasks have completed.