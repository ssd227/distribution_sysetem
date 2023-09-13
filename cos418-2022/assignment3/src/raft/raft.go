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
	"math/rand"
	"src/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister

	// node state
	me    int   // index into peers[]
	state State // node role
	dead  int32 // set by Kill()

	// Your data here.
	CurrentTerm int
	VotedFor    int //初始化为-1，表示没有投票 (or null if none)
	// Log 			[]T

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// help var
	voteNum int

	// channel for machenizem
	// reset two activities timeout channel
	resetElectionTimeoutCh  chan bool
	resetHeartBeatTimeoutCh chan bool
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CurrentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTrem  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// invalid vote requset
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	// new round voting
	if args.Term > rf.CurrentTerm {
		rf.state = Follower
		rf.CurrentTerm = args.Term // update self term
		rf.VotedFor = -1           // reset voteFor= null
	}

	reply.Term = rf.CurrentTerm // same term as vote round

	// If VotedFor is null or candidateId
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		// todo: candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		// if args.LastLogTrem >= rf.CurrentTerm {
		// 	rf.CurrentTerm = args.Term
		// 	reply.term = args.Term
		// 	reply.VoteGranted = true
		// 	return
		// }
		reply.VoteGranted = true
		rf.resetElectionTimeoutCh <- true
		return
	}

	reply.VoteGranted = false
	return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	// Entries		[]T
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	// Your data here.
	Term    int
	Success bool
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// False case1: invalid append
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		
		return
	}

	// update self term
	if args.Term > rf.CurrentTerm {
		rf.state = Follower
		rf.CurrentTerm = args.Term
	}

	reply.Term = rf.CurrentTerm // set reply

	rf.resetElectionTimeoutCh <- true

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	// isLeader := true

	return index, term, rf.state == Leader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// A Raft instance has two time-driven activities:
	go rf.electionActivity()
	go rf.leaderActivity()
}

func (rf *Raft) leaderActivity() {
	/**The paper's Section 5.2 mentions election timeouts in the range
	* of 150 to 300 milliseconds. Such a range only makes sense if
	* the leader sends heartbeats considerably more often than once
	* per 150 milliseconds (e.g., once per 10 milliseconds).
	*
	* Because the tester limits you tens of heartbeats per second,
	* you will have to use an election timeout larger than the
	* paper's 150 to 300 milliseconds, but not too large,
	* because then you may fail to elect a leader within five seconds.
	 */

	// ms := 10  //10ms (config from paper)
	ms := 100 // set for lab-test (the tester limits you tens of heartbeats per second)
	heartBeatTimeout := time.Duration(ms) * time.Millisecond

	for rf.killed() == false {
		select {
		case <-rf.resetHeartBeatTimeoutCh: // used by normal leader append
		case <-time.After(heartBeatTimeout): // election timeout
			go rf.sendHeartBeat()
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	// case not leader
	if rf.state != Leader {
		return
	}
	rf.resetHeartBeatTimeoutCh <- true // reset heart beat timeout
	rf.resetElectionTimeoutCh <- true  // reset election timout 相当于给自己发心跳，避免重新选举

	// case is leader (and send heart beat)
	args := AppendEntriesArgs{} // null args for heartbeat
	args.Term = rf.CurrentTerm

	for i, _ := range rf.peers {
		if rf.me != i {
			go func(peerId int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(peerId, args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) electionActivity() {
	// The election timeout is the amount of time a follower waits until becoming a candidate.

	// ms := (rand.Int63() % 150) + 150 // election timeout 150-300ms  (config from paper)
	ms := (rand.Int63() % 150) + 350 // 350-500ms (set for lab-test)
	electionTimeout := time.Duration(ms) * time.Millisecond

	for rf.killed() == false {
		select {
		case <-rf.resetElectionTimeoutCh:
		case <-time.After(electionTimeout): // election timeout
			go rf.startElection()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate
	rf.CurrentTerm++                  // increment currentTerm
	rf.VotedFor = rf.me               // vote for self
	rf.voteNum = 1                    // reset and count self
	rf.resetElectionTimeoutCh <- true // reset selection timer

	// set value of RequestVoteArgs
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
	}

	// send RequestVote PRC to all other servers
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(peerId int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peerId, args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// 保证过期reply不会更新最新的voteNum
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.state = Follower
						return
					}

					if reply.VoteGranted == true && reply.Term == rf.CurrentTerm {
						rf.voteNum++
						if rf.voteNum > len(rf.peers)/2 { // get major peers's support
							rf.state = Leader
						}
						return
					}
				}
			}(i)
		}
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

	// Your initialization code here.
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		state:       Follower,
		CurrentTerm: 0,
		VotedFor:    -1,

		// help var
		resetElectionTimeoutCh:  make(chan bool),
		resetHeartBeatTimeoutCh: make(chan bool),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker() // main loop logic

	return rf
}
