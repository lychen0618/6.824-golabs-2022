package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"

	// crand "crypto/rand"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/pretty"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type Role int32

const (
	Follower Role = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // 0 at first
	votedFor    int
	logs        []LogEntry
	role        Role
	reElect     bool
	voteCount   int

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int
	cond        *sync.Cond
	applyCh     chan ApplyMsg

	snapshotFlag bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) getPersistState() []byte {
	// assume rf.mu is locked when this func is called
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persistState() {
	// Your code here (2C).
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

// logTerm == -1 indicates that the logEntry with index logIndex must exist
func (rf *Raft) persistStateAndSnapshot(snapshot []byte, logIndex int, logTerm int) {
	// avoid moving states backwards
	if logIndex <= rf.logs[0].Index {
		return
	}
	// get trimmed logs
	trimmedLogs := make([]LogEntry, 0)
	if logTerm == -1 {
		trimmedLogs = append(trimmedLogs, *rf.getLogById(logIndex))
	} else {
		trimmedLogs = append(trimmedLogs, LogEntry{
			Term:    logTerm,
			Index:   logIndex,
			Command: nil})
	}
	for i := logIndex + 1; ; i++ {
		log := rf.getLogById(i)
		if log == nil {
			break
		} else {
			trimmedLogs = append(trimmedLogs, *log)
		}
	}
	rf.logs = trimmedLogs
	if logTerm != -1 {
		rf.snapshotFlag = true
		rf.cond.Signal()
	}
	// save persist data
	state := rf.getPersistState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("Decode Raft's persisted state failed")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Previously, this lab recommended that you implement a function
	// called CondInstallSnapshot to avoid the requirement that snapshots
	// and log entries sent on applyCh are coordinated. This vestigal API
	// interface remains, but you are discouraged from implementing it:
	// instead, we suggest that you simply have it return true.
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persistStateAndSnapshot(snapshot, index, -1)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// call rf.persist according to changed
func (rf *Raft) callPersistIfChanged(changed *bool) {
	if *changed {
		rf.persistState()
	}
}

func (rf *Raft) getLastLog() *LogEntry {
	return &rf.logs[len(rf.logs)-1]
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		reply.Term, reply.VoteGranted = -1, false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	changed := false
	defer rf.callPersistIfChanged(&changed)
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor, rf.role = args.Term, -1, Follower
		changed = true
	}
	lastLog := rf.getLastLog()
	// upToDate equals true indicates that the logs of this follower are more up-to-date
	upToDate := (lastLog.Term > args.LastLogTerm ||
		(lastLog.Term == args.LastLogTerm && lastLog.Index > args.LastLogIndex))
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) ||
		upToDate {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if rf.role == Follower {
		rf.reElect = false
	}
	changed = true
	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	pretty.Debug(pretty.Vote, "S%d granted vote to S%d and became follower", rf.me, args.CandidateId)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term           int
	LeaderId       int
	PrevLogIndex   int
	PrevLogTerm    int
	Logs           []LogEntry
	LeaderCommitId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// for optimization
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	changed := false
	defer rf.callPersistIfChanged(&changed)
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor, rf.role = args.Term, -1, Follower
		changed = true
	}

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	rf.reElect = false
	// If AppendEntries RPC received from new leader: convert to follower
	if rf.role == Candidate {
		pretty.Debug(pretty.Log, "S%d became follower because rev AE/HB from S%d", rf.me, args.LeaderId)
		rf.role = Follower
		if rf.votedFor != -1 {
			changed = true
		}
		rf.votedFor = -1
	}

	// check log consistency
	prevLog := rf.getLogById(args.PrevLogIndex)
	if prevLog != nil && prevLog.Term == args.PrevLogTerm {
		if len(args.Logs) > 0 {
			for i := 1; i <= len(args.Logs); i++ {
				id := i + args.PrevLogIndex
				oneLog := rf.getLogById(id)
				if oneLog == nil || oneLog.Term != args.Logs[i-1].Term {
					// rf.logs[0].Index+l-1==args.Index l==args.Index+1-rf.logs[0].Index  len(args.Logs)-i+1
					rf.logs = append(rf.logs[:args.PrevLogIndex+i-rf.logs[0].Index], args.Logs[i-1:]...)
					changed = true
					break
				}
			}
			pretty.Debug(pretty.Log, "S%d store logs from S%d (len=%d) (prev=%d len=%d)",
				rf.me, args.LeaderId, len(rf.logs)+rf.logs[0].Index-1, args.PrevLogIndex, len(args.Logs))
		}
		if args.LeaderCommitId > rf.commitIndex {
			rf.commitIndex = args.LeaderCommitId
			lastLog := rf.getLastLog()
			if args.LeaderCommitId > lastLog.Index {
				rf.commitIndex = lastLog.Index
			}
			rf.cond.Signal()
		}
		reply.Term, reply.Success = rf.currentTerm, true
	} else {
		// Consistency check failed, so follower should return some info to leader
		// for bypassing conflict entries
		if len(args.Logs) > 0 {
			if prevLog != nil {
				reply.ConflictTerm = prevLog.Term
				reply.ConflictIndex = prevLog.Index
				for i := args.PrevLogIndex - 1; i > 0 && i > rf.commitIndex; i-- {
					if rf.getLogById(i).Term == reply.ConflictTerm {
						reply.ConflictIndex = i
					} else {
						break
					}
				}
			} else {
				reply.ConflictTerm, reply.ConflictIndex = -1, rf.getLastLog().Index+1
			}
		}
		reply.Term, reply.Success = rf.currentTerm, false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// one goroutine will run this function to apply commited logs to channel
func (rf *Raft) applyCommitLogs() {
	for !rf.killed() {
		rf.cond.L.Lock()
		for rf.commitIndex == rf.lastApplied && !rf.snapshotFlag {
			rf.cond.Wait()
		}
		applyMsg := &ApplyMsg{}
		if rf.snapshotFlag {
			if rf.commitIndex < rf.logs[0].Index {
				rf.commitIndex = rf.logs[0].Index
			}
			if rf.lastApplied < rf.logs[0].Index {
				rf.lastApplied = rf.logs[0].Index
			}
			*applyMsg = ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.persister.snapshot,
				SnapshotTerm:  rf.logs[0].Term,
				SnapshotIndex: rf.logs[0].Index,
			}
			rf.snapshotFlag = false
			pretty.Debug(pretty.Apply, "S%d apply snapshot: term %d index %d ",
				rf.me, applyMsg.SnapshotTerm, applyMsg.SnapshotIndex)
		} else {
			rf.lastApplied++
			*applyMsg = ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogById(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			pretty.Debug(pretty.Apply, "S%d apply log entry: command %v index %d",
				rf.me, applyMsg.Command, applyMsg.CommandIndex)
		}
		rf.cond.L.Unlock()
		rf.applyCh <- *applyMsg
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	index, term := -1, -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader {
		pretty.Debug(pretty.Client, "S%d get request: command %v", rf.me, command)
		// create log entry
		lastLog := rf.getLastLog()
		index, term = lastLog.Index+1, rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{term, index, command})
		rf.persistState()
		// send AppendEntries RPC to all servers
		pretty.Debug(pretty.Log, "S%d broadcast AE rpc", rf.me)
		// TODO: replicate batch entries
		go rf.replicateEntries(index, term)
	}
	return index, term, rf.role == Leader
}

func (rf *Raft) getLogById(id int) *LogEntry {
	if id >= rf.logs[0].Index && id < (rf.logs[0].Index+len(rf.logs)) {
		return &rf.logs[id-rf.logs[0].Index]
	} else {
		return nil
	}
}

// [st, ed]
func (rf *Raft) getLogsByRange(st int, ed int) []LogEntry {
	return rf.logs[st-rf.logs[0].Index : ed-rf.logs[0].Index+1]
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// leader will call this function to send its snapshot to those followers far lag behind
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		reply.Term = -1
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.currentTerm, rf.votedFor, rf.role = args.Term, -1, Follower
			rf.persistState()
		}
		reply.Term = rf.currentTerm
		return
	}
	rf.persistStateAndSnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) createAppendEntry(term int, peerId int) *AppendEntriesArgs {
	// cannot use rf.commitIndex directly here
	// or rejoined old leaders may commit wrong entry
	cID := rf.matchIndex[peerId]
	if rf.commitIndex < cID {
		cID = rf.commitIndex
	}
	req := AppendEntriesArgs{
		Term:           term,
		LeaderId:       rf.me,
		PrevLogIndex:   -1,
		LeaderCommitId: cID,
	}
	prevLog := rf.getLogById(rf.nextIndex[peerId] - 1)
	if prevLog != nil {
		req.PrevLogIndex, req.PrevLogTerm = prevLog.Index, prevLog.Term
	}
	return &req
}

func (rf *Raft) replicateEntries(index int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	iters := 0
	// if not send AE after a timeout,
	// the unreliable tests will be very slow
	for !rf.killed() && (rf.role == Leader) && (index == rf.getLastLog().Index) && (rf.currentTerm == term) {
		if iters%10 == 0 {
			pretty.Debug(pretty.Timer, "S%d starts one round of sending AEs", rf.me)
		}
		iters++
		sent := false
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if index >= rf.nextIndex[i] {
				sent = true
				req := rf.createAppendEntry(term, i)
				if req.PrevLogIndex == -1 {
					// have the leader send an InstallSnapshot RPC if it doesn't have
					// the log entries required to bring a follower up to date.
					req := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.logs[0].Index,
						LastIncludedTerm:  rf.logs[0].Term,
						Data:              rf.persister.snapshot,
					}
					go func(i int) {
						pretty.Debug(pretty.Log, "S%d > S%d IS", rf.me, i)
						rsp := new(InstallSnapshotReply)
						ok := rf.sendInstallSnapshot(i, req, rsp)
						if ok {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if (rf.role == Leader) && (index == rf.getLastLog().Index) && (rf.currentTerm == term) {
								if rsp.Term > rf.currentTerm {
									rf.currentTerm, rf.votedFor, rf.role = rsp.Term, -1, Follower
									rf.persistState()
									pretty.Debug(pretty.Log, "S%d became follower because rev IS from S%d", rf.me, i)
								} else {
									rf.nextIndex[i] = req.LastIncludedIndex + 1
								}
							}
						}
					}(i)
				} else {
					req.Logs = rf.getLogsByRange(req.PrevLogIndex+1, index)
					go func(i int) {
						pretty.Debug(pretty.Log, "S%d > S%d AE", rf.me, i)
						rsp := new(AppendEntriesReply)
						ok := rf.sendAppendEntries(i, req, rsp)
						if ok {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if (rf.role == Leader) && (index == rf.getLastLog().Index) && (rf.currentTerm == term) {
								if rsp.Term > rf.currentTerm {
									rf.currentTerm, rf.votedFor, rf.role = rsp.Term, -1, Follower
									rf.persistState()
									pretty.Debug(pretty.Log, "S%d became follower because rev AE from S%d", rf.me, i)
								} else {
									if rsp.Success {
										rf.nextIndex[i], rf.matchIndex[i] = index+1, index
										// see if we can increase commitId (TODO: optimize)
										initialCommitId := rf.commitIndex
										for n := rf.commitIndex + 1; n <= rf.getLastLog().Index; n++ {
											if rf.getLogById(n).Term != rf.currentTerm {
												continue
											}
											count := 1
											for _, mId := range rf.matchIndex {
												if mId >= n {
													count++
												}
											}
											if 2*count > len(rf.peers) {
												rf.commitIndex = n
											} else {
												break
											}
										}
										if rf.commitIndex != initialCommitId {
											pretty.Debug(pretty.Log2, "S%d increase commitID: %d > %d",
												rf.me, initialCommitId, rf.commitIndex)
											rf.cond.Signal()
										}
									} else {
										if rsp.ConflictIndex != 0 { // rpc call may failed because of network
											confLog := rf.getLogById(rsp.ConflictIndex)
											if confLog != nil && confLog.Term == rsp.ConflictTerm {
												rf.nextIndex[i] = rsp.ConflictIndex + 1
											} else {
												rf.nextIndex[i] = rsp.ConflictIndex
											}
										}
										// if rf.nextIndex[i] != 1 {
										// 	rf.nextIndex[i]--
										// }
									}
								}
							}
						}
					}(i)
				}
			}
		}
		if !sent {
			break
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
		rf.mu.Lock()
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		var term int = -1
		if rf.role == Candidate || (rf.reElect && rf.role == Follower) {
			rf.currentTerm++
			rf.role, term, rf.votedFor, rf.voteCount = Candidate, rf.currentTerm, rf.me, 1
			rf.persistState()
			lastLog := rf.getLastLog()
			pretty.Debug(pretty.Log2, "S%d broadcast RequestVote rpc", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(i int, term int, lastLogIndex int, lastLogTerm int) {
						req := RequestVoteArgs{
							Term:         term,
							CandidateId:  rf.me,
							LastLogIndex: lastLogIndex,
							LastLogTerm:  lastLogTerm,
						}
						rsp := RequestVoteReply{}
						ok := rf.sendRequestVote(i, &req, &rsp)
						if ok {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if rf.currentTerm == term && rf.role == Candidate {
								if rsp.Term > rf.currentTerm {
									rf.currentTerm, rf.votedFor, rf.role = rsp.Term, -1, Follower
									rf.persistState()
									pretty.Debug(pretty.Log, "S%d became follower because RV from S%d", rf.me, i)
								} else {
									if rsp.VoteGranted {
										rf.voteCount++
										pretty.Debug(pretty.Vote, "S%d got vote from S%d", rf.me, i)
									}
								}
							}
						}
					}(i, term, lastLog.Index, lastLog.Term)
				}
			}
		}
		if term != -1 {
			go func(term int) {
				rf.mu.Lock()
				for rf.role == Candidate && rf.currentTerm == term {
					if 2*rf.voteCount > len(rf.peers) {
						// become leader: set nextIndex and matchIndex initialy
						pretty.Debug(pretty.Log2, "S%d became leader(%d votes) of term %d", rf.me, rf.voteCount, rf.currentTerm)
						rf.role = Leader
						for i := 0; i < len(rf.nextIndex); i++ {
							rf.nextIndex[i] = rf.logs[0].Index + len(rf.logs)
							rf.matchIndex[i] = 0
						}
						go sendHeartbeats(rf, term)
						break
					}
					rf.mu.Unlock()
					time.Sleep(time.Duration(100) * time.Millisecond)
					rf.mu.Lock()
				}
				rf.mu.Unlock()
			}(term)
		}
		rf.reElect = true
		rf.mu.Unlock()

		// sleep for a random time
		time.Sleep(time.Duration(200+rand.Intn(201)) * time.Millisecond)
	}
}

func sendHeartbeats(rf *Raft, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	iters := 0
	for rf.role == Leader && !rf.killed() {
		if iters%10 == 0 {
			pretty.Debug(pretty.Timer, "S%d is leader, so sends heartbeats", rf.me)
		}
		iters++
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int, term int) {
					rf.mu.Lock()
					req := rf.createAppendEntry(term, i)
					rsp := new(AppendEntriesReply)
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(i, req, rsp)
					if ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.currentTerm == term {
							if rsp.Term > rf.currentTerm {
								rf.currentTerm, rf.votedFor, rf.role = rsp.Term, -1, Follower
								rf.persistState()
								pretty.Debug(pretty.Log, "S%d became follower because rev HB from S%d", rf.me, i)
							}
						}
					}
				}(i, term)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
		rf.mu.Lock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{
		Term:    -1,
		Index:   0,
		Command: nil,
	})
	rf.reElect = true
	rf.role = Follower

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// rf.commitIndex = 0
	// rf.lastApplied = 0
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// rf.snapshot
	rf.snapshotFlag = false

	pretty.Debug(pretty.Log2, "S%d was started", me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.logs[0].Index
	rf.lastApplied = rf.commitIndex

	// start ticker goroutine to start elections
	go rf.ticker()

	// start commit goroutine to apply logs
	go rf.applyCommitLogs()

	return rf
}
