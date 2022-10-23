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
	"github.com/sasha-s/go-deadlock"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

func minInt(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
	Term    int         // Term when entry was received by leader (first index is 1)
	Command interface{} // Command for state machine
}

const ( // metric: ms
	ELECTION_TIMEWAIT_LOW  = 300
	ELECTION_TIMEWAIT_HIGH = 800
	HEARTBEAT_INTERVAL     = 100
)

const (
	LEADER        = 1
	FOLLOWER      = 2
	CANDIDATE     = 3
	PRE_CANDIDATE = 4
	// https://elsonlee.github.io/2019/02/27/etcd-prevote/
)

type AppendEntries struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type HeartbeatReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	log         []LogEntry // log entries;
	state       int
	// Volatile state on all servers
	commitIndex int //index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of the highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically

	// It's easiest to use time.Sleep() with a small constant argument to drive the periodic checks.
	// Don't use time.Ticker and time.Timer; they are tricky to use correctly.
	HeartbeatSentTime time.Time // For LEADER: last time received heartbeat
	HeartbeatTime     time.Time // For FOLLOWER: last time sent heartbeat to FOLLOWER s

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
func (rf *Raft) checkCommit() {
	for N := len(rf.log) - 1; N > rf.commitIndex && rf.log[N].Term == rf.currentTerm; N-- {
		nReplicated := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				nReplicated += 1
			}
			if nReplicated > len(rf.peers)/2 {
				rf.commitIndex = N
				Debug(dLog, "S%d exists major commit with index %d to apply", rf.me, rf.commitIndex)
				go rf.Apply2StateMachine()
				break
			}
		}
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = FOLLOWER
	rf.HeartbeatTime = time.Now()
	// Each server will vote for at most one candidate in a given term
	if term >= rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
}

func (rf *Raft) becomeCandidate(pre bool) {
	rf.mu.Lock()
	if pre {
		rf.state = PRE_CANDIDATE
		rf.votedFor = -1
	} else {
		rf.state = CANDIDATE
		rf.votedFor = rf.me
		rf.currentTerm++
	}
	rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// UP_TO_DATE restriction
	Debug(dVote, "S%d(%d)<-%d processing received vote request for term %d", rf.me, rf.state, args.CandidateID, args.Term)
	term, isLeader := rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = term
	if args.Term < term && args.LastLogIndex != 0 {
		reply.VoteGranted = false
		Debug(dVote, "S%d->%d vote False due to TERM %d ahead of %d", rf.me, args.CandidateID, rf.currentTerm, args.Term)
		return
	}
	if args.Term == term && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		Debug(dVote, "S%d->%d vote False due to votedFor %d not %d", rf.me, args.CandidateID, rf.votedFor, args.CandidateID)
		return
	}
	if args.Term > term {
		Debug(dDrop, "S%d(%d) to FOLLOWER due to TERM %d out-of-date %d", rf.me, rf.state, rf.currentTerm, args.Term)
		rf.becomeFollower(args.Term)
	}

	LastLogIndex := len(rf.log) - 1
	LastLogTerm := rf.log[LastLogIndex].Term
	Debug(dVote, "S%d(%d)<-%d checked term for vote request for term %d", rf.me, rf.state, args.CandidateID, args.Term)
	if args.LastLogTerm < LastLogTerm {
		reply.VoteGranted = false
		Debug(dVote, "S%d->%d FOLLOWER vote False LastLogTERM %d not up-to-date with %d",
			rf.me, args.CandidateID, LastLogTerm, args.LastLogTerm)
		return
	} else if rf.log[LastLogIndex].Term == args.LastLogTerm && args.LastLogIndex < LastLogIndex {
		reply.VoteGranted = false
		Debug(dVote, "S%d->%d FOLLOWER vote False LastLogIndex %d not up-to-date with %d",
			rf.me, args.CandidateID, LastLogIndex, args.LastLogTerm)
		return
	} else if isLeader && rf.log[LastLogIndex].Term == args.LastLogTerm && args.LastLogIndex == LastLogIndex {
		reply.VoteGranted = false
		Debug(dVote, "S%d->%d FOLLOWER vote False LastLogIndex %d not up-to-date with %d",
			rf.me, args.CandidateID, LastLogIndex, args.LastLogTerm)
		return
	}

	rf.HeartbeatTime = time.Now()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	Debug(dVote, "S%d(%d)->%d vote True", rf.me, rf.state, args.CandidateID)
	return
}

func (rf *Raft) RequestHeartbeat(args *AppendEntries, reply *HeartbeatReply) {
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	term, isLeader := rf.GetState()
	rf.mu.Lock()
	rf.HeartbeatTime = time.Now()
	reply.Term = term
	if args.Term < term { //outdated leader
		Debug(dDrop, "S%d(Term: %d) got heartbeat from outdated TERM %d, discard", rf.me, term, args.Term)
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	if args.Term > term { // give up leader because others is
		//DPrintf("%d changed to FOLLOWER due to term %d out-of-date %d", rf.me, term, args.Term)
		Debug(dDrop, "S%d FOLLOWER(TERM %d) out-of-date %d or give up identity", rf.me, term, args.Term)
		rf.becomeFollower(args.Term)
	}
	if isLeader && args.Term == term { // received another LEADER's message? turn to FOLLOWER
		Debug(dDrop, "S%d LEADER(Term: %d) got heartbeat from same TERM %d, turn to FOLLOWER", rf.me, term, args.Term)
		rf.becomeFollower(term)
		rf.mu.Unlock()
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		Debug(dError, "S%d<-%d FOLLOWER heartbeat PrevLogIndex %d, exceeds len(logs) %d(with %d logs)",
			rf.me, args.LeaderId, args.PrevLogIndex, len(rf.log), len(args.Entries))
		rf.mu.Unlock()
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		Debug(dError, "S%d<-%d FOLLOWER heartbeat mismatch PrevLogTerm, actual %d, given %d, prevLogIndex: %d(with %d logs)",
			rf.me, args.LeaderId, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries))
		reply.Success = false
		rf.log = rf.log[:args.PrevLogIndex]
		rf.mu.Unlock()
		return
	}
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// If the follower has all the entries the leader sent, the follower MUST NOT truncate its log.
	// Any elements following the entries sent by the leader MUST be kept.
	// Here I start comparing from "PrevLogIndex".
	if len(args.Entries) > 0 {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	}
	endIdx := len(rf.log) - 1
	rf.mu.Unlock()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, endIdx)
		go rf.Apply2StateMachine()
	}
	reply.Success = true
	Debug(dInfo, "S%d<-S%d received Heartbeat term %d with %d logs(prevLog:%d -> ends logs:%d), reply %t, update log to %v",
		rf.me, args.LeaderId, args.Term, len(args.Entries), args.PrevLogIndex, len(rf.log)-1, reply.Success, rf.log)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntries, reply *HeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.RequestHeartbeat", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logEntry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, logEntry)
	Debug(dLeader, "S%d LEADER got a log command %v（Entry Term %d), now have %d logs",
		rf.me, command, logEntry.Term, len(rf.log))
	return len(rf.log) - 1, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// startElection sending out RequestVote RPCs
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.HeartbeatTime = time.Now()
	term := rf.currentTerm // Term may not be the same as the rf.currentTerm at which the surrounding code decided to become a Candidate.
	if rf.state == PRE_CANDIDATE {
		term++
	}
	rf.mu.Unlock()
	requestVoteArgs := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	//DPrintf("%d Start an election(for term: %d).", rf.me, term)
	Debug(dVote, "S%d(%d) start election(for TERM: %d) with LastLogTerm %d.",
		rf.me, rf.state, term, requestVoteArgs.LastLogTerm)

	TrueVotes := 1 // must vote self, so init 1.
	votesLock := deadlock.Mutex{}
	cond := sync.NewCond(&votesLock)
	finish := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// vote for self.
			continue
		}
		go func(i int) {
			requestVoteReply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, requestVoteArgs, requestVoteReply)
			Debug(dVote, "S%d(%d)->%d send vote request ", rf.me, rf.state, i)
			if ok {
				Debug(dVote, "S%d(%d)<-%d receive vote response %v", rf.me, rf.state, i, requestVoteReply.VoteGranted)
				if requestVoteReply.Term > term {
					rf.mu.Lock()
					rf.becomeFollower(requestVoteReply.Term)
					rf.mu.Unlock()
					cond.Broadcast()
					finish++
					return
				}
				// DPrintf("Request vote(%t) %d/%d by candidate %d.", requestVoteReply.VoteGranted, i, len(rf.peers), rf.me)
				votesLock.Lock()
				if requestVoteReply.VoteGranted {
					TrueVotes++
				}
				votesLock.Unlock()
			}
			cond.Broadcast()
			finish++
		}(i)
	}
	votesLock.Lock()
	for TrueVotes <= len(rf.peers)/2 && finish != len(rf.peers) {
		cond.Wait()
	}
	// DPrintf("%d's election ends with votes %d.", rf.me, votes)
	if TrueVotes > len(rf.peers)/2 {
		Debug(dVote, "S%d CANDIDATE(%d) election success(VOTEs:%d).", rf.me, rf.state, TrueVotes)
		if rf.state == CANDIDATE {
			// When a leader first comes to power,
			// it initializes all nextIndex values to the index just after the last one in its log (11 in Figure 7).
			rf.mu.Lock()
			Debug(dLeader, "S%d LEADER for TERM %d, establishing its authority", rf.me, rf.currentTerm)
			rf.state = LEADER
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			rf.mu.Unlock()
			go rf.Heartbeat() // send initial empty AppendEntries RPCs
		} else if rf.state == PRE_CANDIDATE {
			rf.becomeCandidate(false)
			defer rf.startElection()
		}
	} else {
		Debug(dVote, "S%d CANDIDATE(%d) election failed.(VOTEs:%d).", rf.me, rf.state, TrueVotes)
		rf.becomeFollower(term - 1)
	}
	votesLock.Unlock()
	if rf.state == FOLLOWER {
		rf.votedFor = -1
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		_, isLeader := rf.GetState()
		r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me*114514)))
		election_timeout := r.Float64()*(ELECTION_TIMEWAIT_HIGH-ELECTION_TIMEWAIT_LOW) + ELECTION_TIMEWAIT_LOW
		if rf.state == FOLLOWER && time.Since(rf.HeartbeatTime) > time.Duration(election_timeout)*time.Millisecond {
			rf.becomeCandidate(true)
			go rf.startElection()
			//	if rf.state == CANDIDATE && rf.votedFor == -1 {
			//		go rf.startElection()
			//	} else {
			//		rf.becomeCandidate(true)
			//		go rf.startElection()
			//	}
		}
		if isLeader && time.Since(rf.HeartbeatSentTime) > HEARTBEAT_INTERVAL*time.Millisecond {
			go rf.Heartbeat()
			go rf.checkCommit()
		}
		time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
	}
}

func (rf *Raft) buildHeartbeatInfo(peer int) *AppendEntries {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prevLogIndex := 0
	if rf.matchIndex[peer] == 0 { // for new Leader to use
		prevLogIndex = rf.nextIndex[peer] - 1
	} else {
		prevLogIndex = rf.matchIndex[peer]
	}
	prevLogTerm := rf.log[prevLogIndex].Term

	var entries []LogEntry
	if prevLogIndex+1 < len(rf.log) {
		entries = rf.log[prevLogIndex+1:]
	}
	return &AppendEntries{rf.currentTerm, rf.me, prevLogIndex,
		prevLogTerm, entries, rf.commitIndex}
}

func (rf *Raft) Heartbeat() {
	// no need to : var wg sync.WaitGroup
	term, isLeader := rf.GetState()
	if !isLeader {
		return
	}
	rf.HeartbeatSentTime = time.Now()
	votes := 1 // must vote self, so init 1.
	votesLock := deadlock.Mutex{}
	cond := sync.NewCond(&votesLock)
	finish := 1
	endIdx := len(rf.log) - 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// checked above: still leader?
			continue
		}
		go func(i int) {
			if rf.state != LEADER {
				finish++
				cond.Broadcast()
				return
			}
			appendEntries := rf.buildHeartbeatInfo(i)
			Debug(dTrace, "S%d->S%d LEADER(term %d) send heartbeat with PrevLogIndex %d, to %d",
				rf.me, i, term, appendEntries.PrevLogIndex, endIdx)
			//Decoding into a non-default variable/field Term may not work
			heartbeatReply := &HeartbeatReply{}
			ok := rf.sendHeartbeat(i, appendEntries, heartbeatReply)
			for ok && !heartbeatReply.Success {
				if heartbeatReply.Term > term {
					rf.mu.Lock()
					rf.becomeFollower(heartbeatReply.Term)
					rf.mu.Unlock()
					Debug(dDrop, "S%d<-S%d LEADER retired since heartbeat reply term(%d) > rf.currentTerm %d, PrevLogTerm %d",
						rf.me, i, heartbeatReply.Term, term, appendEntries.PrevLogTerm)
					break
				} else if heartbeatReply.Term == -1 {
					Debug(dDrop, "S%d<!-S%d LEADER(term %d) heartbeat outdated",
						rf.me, i, rf.currentTerm)
					break
				} else if heartbeatReply.Term < term {
					Debug(dLeader, "S%d<-S%d LEADER heartbeat reply term(%d) mismatch, nextIdx=%d, PrevLogTerm %d",
						rf.me, i, term, rf.nextIndex[i], appendEntries.PrevLogTerm)
					break
				}
				rf.mu.Lock()
				if rf.nextIndex[i] > 1 {
					rf.nextIndex[i]--
				}
				rf.mu.Unlock()
				appendEntries = rf.buildHeartbeatInfo(i)
				Debug(dLeader, "S%d<-S%d LEADER heartbeat reply(%t), retry with nextIdx=%d, PrevLogTerm %d",
					rf.me, i, heartbeatReply.Success, rf.nextIndex[i], appendEntries.PrevLogTerm)
				heartbeatReply = &HeartbeatReply{}
				ok = rf.sendHeartbeat(i, appendEntries, heartbeatReply)
			}
			if heartbeatReply.Success && heartbeatReply.Term == term {
				// If successful: update nextIndex and matchIndex for follower (§5.3)
				votesLock.Lock()
				votes++
				votesLock.Unlock()
				rf.mu.Lock()
				// Don't use this, for appendEntries.PrevLogIndex may change due to delete FOLLOWER
				rf.matchIndex[i] = appendEntries.PrevLogIndex + len(appendEntries.Entries)
				rf.nextIndex[i] = len(rf.log)
				rf.mu.Unlock()
				Debug(dLeader, "S%d<-S%d LEADER heartbeat reply(%t), set its nextIdx=%d, matchIdx=%d",
					rf.me, i, heartbeatReply.Success, rf.nextIndex[i], rf.matchIndex[i])
			}
			finish++
			cond.Broadcast()
		}(i)
	}
	votesLock.Lock()
	for votes <= len(rf.peers)/2 && finish != len(rf.peers) {
		cond.Wait()
	}
	if rf.state != LEADER {
		finish++
		cond.Broadcast()
		return
	}
	Debug(dLeader, "S%d(%d) AppendEntry ends(VOTEs:%d).", rf.me, rf.state, votes)
	if votes > len(rf.peers)/2 {
		if endIdx != rf.commitIndex {
			rf.commitIndex = endIdx
			Debug(dCommit, "S%d LEADER AppendEntry commits(TERM:%d, commitIndex:%d).", rf.me, rf.currentTerm, rf.commitIndex)
		}
		go rf.Apply2StateMachine()
	}
	votesLock.Unlock()
}

func (rf *Raft) Apply2StateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for applyIdx := rf.lastApplied + 1; applyIdx <= rf.commitIndex; applyIdx++ {
		applyMsg := ApplyMsg{
			Command:      rf.log[applyIdx].Command,
			CommandIndex: applyIdx,
			CommandValid: true,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
		rf.lastApplied = applyIdx
		// 当领导者将日志项成功复制至集群大多数节点的时候，日志项处于 committed 状态，领导者可将这个日志项应用（apply）到自己的状态机中
		Debug(dLog2, "S%d applys(TERM:%d, lastApplied->%d) command %v.",
			rf.me, rf.currentTerm, rf.lastApplied, rf.log[applyIdx].Command)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service xpects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.log = append(rf.log, LogEntry{Term: 0}) // a naive item to make idx start from 1.
	rf.state = FOLLOWER                        // When servers start up, they begin as followers.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.HeartbeatTime = time.Now()
	rf.HeartbeatSentTime = time.Now()
	go rf.ticker()

	return rf
}
