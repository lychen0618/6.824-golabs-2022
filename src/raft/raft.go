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
	//	"bytes"
	// crand "crypto/rand"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	// decide if the logs of this follower is more up-to-date
	lastLogIndex := len(rf.logs)
	lastLogTerm := 0
	if lastLogIndex != 0 {
		lastLogTerm = rf.logs[lastLogIndex-1].Term
	}
	upToDate := (lastLogTerm > args.LastLogTerm ||
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex))
	if args.Term < rf.currentTerm ||
		(rf.votedFor != -1 && rf.votedFor != args.CandidateId) ||
		upToDate {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.role == Follower {
		rf.reElect = false
	}
	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	pretty.Debug(pretty.Vote, "S%d granted vote to S%d", rf.me, args.CandidateId)
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	if args.Term == rf.currentTerm {
		rf.reElect = false
		if rf.role == Candidate {
			rf.role = Follower
			rf.votedFor = -1
		}
	}
	reply.Term = rf.currentTerm
	// check log consistency
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.PrevLogIndex == 0 ||
		(len(rf.logs) >= args.PrevLogIndex &&
			rf.logs[args.PrevLogIndex-1].Term == args.PrevLogTerm) {
		if len(args.Logs) > 0 {
			reply.Success = true
			rf.logs = rf.logs[0:args.PrevLogIndex]
			rf.logs = append(rf.logs, args.Logs...)
			pretty.Debug(pretty.Log, "S%d store logs from S%d", rf.me, args.LeaderId)
		}
		if args.LeaderCommitId > rf.commitIndex {
			rf.commitIndex = args.LeaderCommitId
			if args.LeaderCommitId > len(rf.logs) {
				rf.commitIndex = len(rf.logs)
			}
			rf.cond.Signal()
		}
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
		for rf.commitIndex == rf.lastApplied {
			rf.cond.Wait()
		}
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		pretty.Debug(pretty.Apply, "S%d apply: command %v index %d",
			rf.me, applyMsg.Command, applyMsg.CommandIndex)
		rf.lastApplied++
		rf.cond.L.Unlock()
		rf.applyCh <- applyMsg
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
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	isLeader = (rf.role == Leader)
	if isLeader {
		pretty.Debug(pretty.Client, "S%d get request: command %v", rf.me, command)
		index = len(rf.logs) + 1
		term = rf.currentTerm
		// create log entry
		rf.logs = append(rf.logs, LogEntry{term, command})
		// send AppendEntries RPC to all servers
		pretty.Debug(pretty.Log, "S%d broadcast AE rpc", rf.me)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(rf *Raft, index int, term int, i int) {
				rf.mu.Lock()
				// must check if index equals len(rf.logs)
				// or TestConcurrentStarts2B will fail
				for (index >= rf.nextIndex[i]) && (rf.role == Leader) && (index == len(rf.logs)) {
					prevIndex := rf.nextIndex[i] - 1
					prevTerm := -1
					if prevIndex > 0 {
						prevTerm = rf.logs[prevIndex-1].Term
					}
					req := AppendEntriesArgs{
						Term:           term,
						LeaderId:       rf.me,
						PrevLogIndex:   prevIndex,
						PrevLogTerm:    prevTerm,
						Logs:           rf.logs[prevIndex:index],
						LeaderCommitId: rf.commitIndex,
					}
					rsp := AppendEntriesReply{}
					rf.mu.Unlock()
					pretty.Debug(pretty.Log, "S%d > S%d AE", rf.me, i)
					ok := rf.sendAppendEntries(i, &req, &rsp)
					rf.mu.Lock()
					if ok {
						if rsp.Term > rf.currentTerm {
							rf.role = Follower
							rf.currentTerm = rsp.Term
							rf.votedFor = -1
							pretty.Debug(pretty.Log, "S%d became follower because rev AE from S%d", rf.me, i)
						} else if (rf.role == Leader) && (index == len(rf.logs)) {
							if rsp.Success {
								rf.nextIndex[i] = index + 1
								rf.matchIndex[i] = index
								// see if we can increase commitId
								initialCommitId := rf.commitIndex
								for n := rf.commitIndex + 1; n <= len(rf.logs); n++ {
									if rf.logs[n-1].Term != rf.currentTerm {
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
								if rf.nextIndex[i] != 1 {
									rf.nextIndex[i]--
								}
							}
						}
					}
				}
				rf.mu.Unlock()
			}(rf, index, term, i)
		}
	}
	rf.mu.Unlock()
	return index, term, isLeader
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
			rf.role = Candidate
			rf.currentTerm++
			term = rf.currentTerm
			rf.votedFor = rf.me
			rf.voteCount = 1
			lastLogIndex := len(rf.logs)
			lastLogTerm := 0
			if lastLogIndex != 0 {
				lastLogTerm = rf.logs[lastLogIndex-1].Term
			}
			pretty.Debug(pretty.Log2, "S%d broadcast RequestVote rpc", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(rf *Raft, i int, term int, lastLogIndex int, lastLogTerm int) {
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
							if rsp.Term > rf.currentTerm {
								rf.role = Follower
								rf.currentTerm = rsp.Term
								rf.votedFor = -1
								pretty.Debug(pretty.Log, "S%d became follower because RV from S%d", rf.me, i)
							}
							if rsp.VoteGranted && rf.role == Candidate {
								rf.voteCount++
								pretty.Debug(pretty.Vote, "S%d got vote from S%d", rf.me, i)
							}
						}
					}(rf, i, term, lastLogIndex, lastLogTerm)
				}
			}
		}
		rf.reElect = true
		rf.mu.Unlock()

		// sleep for a random time
		time.Sleep(time.Duration(300+rand.Intn(201)) * time.Millisecond)

		rf.mu.Lock()
		if rf.role == Candidate && rf.currentTerm == term && 2*rf.voteCount > len(rf.peers) {
			// become leader: set nextIndex and matchIndex initialy
			pretty.Debug(pretty.Log2, "S%d became leader(%d votes) of term %d", rf.me, rf.voteCount, rf.currentTerm)
			rf.role = Leader
			for i := 0; i < len(rf.nextIndex); i++ {
				rf.nextIndex[i] = len(rf.logs) + 1
				rf.matchIndex[i] = 0
			}
			go sendHeartbeats(rf, term)
		}
		rf.mu.Unlock()
	}
}

func sendHeartbeats(rf *Raft, term int) {
	rf.mu.Lock()
	iters := 0
	for rf.role == Leader && !rf.killed() {
		if iters%10 == 0 {
			pretty.Debug(pretty.Timer, "S%d is leader, so sends heartbeats", rf.me)
		}
		iters++
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				// cannot use rf.commitIndex directly here
				// or rejoined old leaders may commit wrong entry
				cID := rf.matchIndex[i]
				if rf.commitIndex < cID {
					cID = rf.commitIndex
				}
				go func(rf *Raft, i int, term int, commitId int) {
					rf.mu.Lock()
					prevIndex := rf.nextIndex[i] - 1
					prevTerm := -1
					if prevIndex > 0 {
						prevTerm = rf.logs[prevIndex-1].Term
					}
					req := AppendEntriesArgs{
						Term:           term,
						LeaderId:       rf.me,
						PrevLogIndex:   prevIndex,
						PrevLogTerm:    prevTerm,
						LeaderCommitId: commitId,
					}
					rsp := AppendEntriesReply{}
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(i, &req, &rsp)
					if ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rsp.Term > rf.currentTerm {
							rf.role = Follower
							rf.currentTerm = rsp.Term
							rf.votedFor = -1
							pretty.Debug(pretty.Log, "S%d became follower because rev HB from S%d", rf.me, i)
						}
					}
				}(rf, i, term, cID)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(50) * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
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
	rf.reElect = true
	rf.role = Follower

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	pretty.Debug(pretty.Log2, "S%d was started", me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start commit goroutine to apply logs
	go rf.applyCommitLogs()

	return rf
}
