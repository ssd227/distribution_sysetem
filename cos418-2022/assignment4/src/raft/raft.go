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
	"encoding/gob"
	"math/rand"
	"src/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Follower State = iota
	Candidate
	Leader
)

type State int

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogList []LogEntry

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// tool var
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	applyCh   chan ApplyMsg

	// node state
	me    int   // index into peers[]
	state State // node role
	dead  int32 // set by Kill()

	// persistent state on all servers
	//(updated on stable sttorage before responding to RPCs)
	CurrentTerm int
	VotedFor    int // -1 for none vote
	Log         LogList

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int
	matchIndex []int

	// help var
	voteNum int

	// reset two activities timeout channel
	resetElectionTimeoutCh  chan bool
	resetHeartBeatTimeoutCh chan bool
}

// return currentTerm and whether this server
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
	// 调用本函数时,保证外部函数加锁
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	// Debug(dPersist, "S%d save to ss CT[%v]VF[%v]Log[%v]",
	// 	rf.me, rf.CurrentTerm,
	// 	rf.VotedFor,
	// 	rf.Log)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)

	Debug(dPersist, "S%d read from ss CT[%v] VF[%v] Log[%v-%v]",
		rf.me, rf.CurrentTerm,
		rf.VotedFor,
		len(rf.Log),
		rf.Log.lastn())
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

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && // If VotedFor is null or candidateId
		(args.LastLogTrem > rf.Log[len(rf.Log)-1].Term || // candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)) {
			args.LastLogTrem == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex >= (len(rf.Log)-1)) {

		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.resetElectionTimeoutCh <- true // granting vote to candidate
		Debug(dVote, "S%v at T%v Granting Vote to S%v ", rf.me, rf.CurrentTerm, args.CandidateId)

		rf.persist()
		return
	}

	reply.VoteGranted = false

	rf.persist()
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term              int
	Success           bool
	InconsistentTerm  int
	InconsistentIndex int
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// False case1: invalid append
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		// Debug(dClient, "S%d Started at T:%v LLI:", rf.me, rf.CurrentTerm, len(rf.Log)-1)
		return
	}

	// update self term
	if args.Term > rf.CurrentTerm {
		rf.state = Follower
		rf.CurrentTerm = args.Term
	}

	reply.Term = rf.CurrentTerm // set reply

	rf.resetElectionTimeoutCh <- true // !!(易错)!! 不一致不表示append失败，也需要重置选举时间

	// False case2: inconsistence
	if !(args.PrevLogIndex < len(rf.Log) &&
		rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm) { // note: symbol[!] before bool expression

		reply.Success = false // set reply

		// 优化不一致的情况，每次跳过一个term (实际工程不推荐使用)
		if args.PrevLogIndex >= len(rf.Log) {
			reply.InconsistentTerm = rf.Log[len(rf.Log)-1].Term
			reply.InconsistentIndex = len(rf.Log)
		} else {
			reply.InconsistentTerm = rf.Log[args.PrevLogIndex].Term

			newId := args.PrevLogIndex
			for rf.Log[newId].Term == rf.Log[newId-1].Term {
				newId--
			}
			reply.InconsistentIndex = newId
		}

		rf.persist()
		return
	}

	reply.Success = true // set reply

	// step1: keep same entry already in rf.Log
	i := 0
	for i < len(args.Entries) {
		if ((args.PrevLogIndex + 1 + i) < len(rf.Log)) &&
			(args.Entries[i].Term == rf.Log[args.PrevLogIndex+1+i].Term) {
			i++
			continue
		} else {
			rf.Log = rf.Log[:args.PrevLogIndex+1+i] // trunct inconsistent log
			break
		}
	}
	// step2: 对齐log后，从i位置开始append new entries
	rf.Log = append(rf.Log, args.Entries[i:]...) // 一次append多个元素

	Debug(dLog, "S%d at T%v Log[%v-%v]", rf.me, rf.CurrentTerm, len(rf.Log), rf.Log.lastn())

	// update rf.commitIndex and rf.lastApplied
	if args.LeaderCommit > rf.commitIndex { // 对齐了log才能更新，否则出错!!(易错)!!
		rf.commitIndex = Min(args.LeaderCommit, len(rf.Log)-1)
		rf.updateLastApplied()
		Debug(dLog2, "S%d at T%v CI[%v],LA[%v],Log[%v-%v]",
			rf.me, rf.CurrentTerm,
			rf.commitIndex, rf.lastApplied,
			len(rf.Log), rf.Log.lastn())
	}

	rf.persist()
	return
}

func (rf *Raft) updateLastApplied() {
	for rf.commitIndex > rf.lastApplied {
		rf.applyCh <- ApplyMsg{Index: rf.lastApplied + 1, Command: rf.Log[rf.lastApplied+1].Command}
		rf.lastApplied++
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

/****************************************** leader funcs ****************************************/

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
		case <-time.After(heartBeatTimeout): // heartbeat timeout
			go rf.leaderAppendEntries(true)
		}
	}
}

func (rf *Raft) leaderAppendEntries(isHeartBeats bool) {
	if rf.state != Leader {
		return
	}
	rf.resetHeartBeatTimeoutCh <- true // reset heart beat timeout
	rf.resetElectionTimeoutCh <- true  // reset election timout 相当于给自己发心跳，避免重新选举

	// leader send heart beat
	for i, _ := range rf.peers {
		if rf.me != i {
			go func(peerId int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[peerId] - 1,
					PrevLogTerm:  rf.Log[rf.nextIndex[peerId]-1].Term,
					LeaderCommit: rf.commitIndex,
				}

				// TODO：fix1：heart beat不能强制设为nil list，让它干活，否则对不齐log（目前修正版本）
				// 		fix2：或者在接受端处理 空entries && 对齐log 这种特殊情况，需要直接删掉尾巴
				if len(rf.Log) > rf.nextIndex[peerId] {
					args.Entries = rf.Log[rf.nextIndex[peerId]:] // todo 传输同步日志过长，可能存在的问题
				} else { // empty Log for heartbeat || no more log needed to send
					args.Entries = []LogEntry{}
				}
				rf.mu.Unlock()

				if !isHeartBeats {
					Debug(dLog, "S%v->S%d at T%v send Entries[%v]",
						rf.me, peerId, rf.CurrentTerm, args.Entries)
				} else {
					Debug(dLog, "S%v->S%d at T%v send heartbeat[PLI:%v PLT:%v LC:%v]",
						rf.me, peerId, rf.CurrentTerm,
						args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
				}

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peerId, args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.state = Follower
						return
					}

					if reply.Success { // update nextIndex and matchIndex for follower
						rf.nextIndex[peerId] = Max(rf.nextIndex[peerId], args.PrevLogIndex+len(args.Entries)+1)
						rf.matchIndex[peerId] = Max(rf.matchIndex[peerId], args.PrevLogIndex+len(args.Entries))
						if !isHeartBeats {
							Debug(dLog, "S%v<-S%v at T%v get_reply[Success] NI:%v MI:%v",
								rf.me, peerId, rf.CurrentTerm,
								rf.nextIndex[peerId],
								rf.matchIndex[peerId])
						}

					} else if reply.Term == args.Term { // distinct two append false return cases
						rf.nextIndex[peerId] = reply.InconsistentIndex // log inconsistency, decrement nextIndex
						if !isHeartBeats {
							Debug(dLog, "S%v<-S%d at T%v, get_reply[False][log inconsistency] NI:%v MI:%v",
								rf.me, peerId, rf.CurrentTerm,
								rf.nextIndex[peerId],
								rf.matchIndex[peerId])
						}
					}
					go rf.leaderUpdateCommitInedx()
					return
				}
			}(i)
		}
	}
}

func (rf *Raft) leaderUpdateCommitInedx() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for N := rf.commitIndex + 1; true; {
		count := 0
		for _, v := range rf.matchIndex {
			if v >= N {
				count++
			}
		}

		if count >= (len(rf.peers)-1)/2 { // 不包含自己超过一半，即可commit
			if rf.Log[N].Term == rf.CurrentTerm {
				rf.commitIndex = N
			}
			N++
		} else {
			break // 结束循环
		}
	}

	rf.updateLastApplied()
	Debug(dLog2, "S%d at T%v LCI[%v] LA[%v] Log[%v-%v]",
		rf.me, rf.CurrentTerm,
		rf.commitIndex, rf.lastApplied,
		len(rf.Log), rf.Log.lastn())
}

/********************************************** end ****************************************************/

/****************************************** election funcs ****************************************/
func (rf *Raft) electionActivity() {
	// The election timeout is the amount of time a follower waits until becoming a candidate.

	// ms := (rand.Int63() % 150) + 150 // election timeout 150-300ms  (config from paper)
	ms := (rand.Int63() % 150) + 350 // 350-500ms (set for lab-test)
	electionTimeout := time.Duration(ms) * time.Millisecond

	for rf.killed() == false {
		// if rf.state == Leader { continue } 这么控制leader没有意义吧。是否进入选举状态全靠 electtimeout 决定。
		select {
		case <-rf.resetElectionTimeoutCh:
		case <-time.After(electionTimeout): // election timeout
			go rf.electionStart()
		}
	}
}

func (rf *Raft) electionStart() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate
	rf.CurrentTerm++                  // increment currentTerm
	rf.VotedFor = rf.me               // vote for self
	rf.voteNum = 1                    // reset and count self
	rf.resetElectionTimeoutCh <- true // reset selection timer

	Debug(dVote, "S%d 开始选举 at T%v", rf.me, rf.CurrentTerm)
	rf.persist() // update on stable storage before responding to RPCs

	// set value of RequestVoteArgs
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Log) - 1, // todo 这种处理方法不适用于 log压缩，需要 += len(压缩前log)
		LastLogTrem:  rf.Log[len(rf.Log)-1].Term,
	}

	// send RequestVote PRC to all other servers
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(peerId int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peerId, args, &reply)
				if ok {
					rf.mu.Lock() // 保证读取结果在raft里更新互斥
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
							if rf.state != Leader { // 第一次当选Leader, 初始化对应参数
								// 选举成功, init leader参数
								rf.state = Leader
								rf.matchIndex = make([]int, len(rf.peers)) // init by 0
								rf.nextIndex = make([]int, len(rf.peers))
								for i, _ := range rf.nextIndex {
									rf.nextIndex[i] = len(rf.Log) // init by last_index(of current log) + 1
								}
							}
							Debug(dLeader, "S%d 获得大多数投票 at T%v(%v), converting to Leader", rf.me, rf.CurrentTerm, rf.voteNum)
						}
						return
					}
				}
			}(i)
		}
	}
}

/********************************************** end ****************************************************/

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
	if rf.state != Leader {
		return 0, 0, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Log = append(rf.Log, LogEntry{Term: rf.CurrentTerm, Command: command})

	Debug(dClient, "S%d at T%v get client call command[%v] log_len[%v],log[%v-%v]",
		rf.me, rf.CurrentTerm, command, len(rf.Log), len(rf.Log), rf.Log.lastn())

	go rf.leaderAppendEntries(false)

	rf.persist() // update on stable storage before responding to RPCs
	return len(rf.Log) - 1, rf.CurrentTerm, true
}

func (rf *Raft) ticker() {
	// A Raft instance has two time-driven activities:
	go rf.electionActivity()
	go rf.leaderActivity()
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
	
	//pretty-debug init 
	debugInit()

	// Your initialization code here.
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,

		state:       Follower,
		CurrentTerm: 0,
		VotedFor:    -1,
		Log:         []LogEntry{LogEntry{Term: 0}}, // add nil LogEntry in Log

		// volatile state on all servers
		commitIndex: 0,
		lastApplied: 0,

		// volatile state on leaders: [not init here！在选举成功后初始化]

		// help var
		resetElectionTimeoutCh:  make(chan bool),
		resetHeartBeatTimeoutCh: make(chan bool),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	Debug(dClient, "S%d Started at T%v LLI:%v", rf.me, rf.CurrentTerm, len(rf.Log)-1)

	// start ticker goroutine to start elections
	go rf.ticker() // main loop logic

	return rf
}
