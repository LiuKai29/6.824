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

	//"crypto/rand"
	//"fmt"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"

	//	"6.824/labgob"
	"6.824/labrpc"
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
	Command interface{}
	Term    int
}

type State int

const (
	Follower State = iota
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
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeartbeat     time.Time
	electionTimeout   time.Duration
	lastHeartbeatSent time.Time

	state State

	snapShot []byte
	applyCh  chan ApplyMsg

	// 所有 Index 均代表 Absolute Index, 即相对于整个日志的 Index
	// 访问 log 时需要使用 log[index - offsetIndex] 来访问
	offsetIndex int
	offsetTerm  int
}

func (rf *Raft) AbsoluteIndexToLogIndex(absoluteIndex int) int {
	if absoluteIndex <= rf.offsetIndex {
		return 0
	} else {
		return absoluteIndex - rf.offsetIndex
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// persist 加上 offsetIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	for _, value := range []interface{}{rf.currentTerm, rf.votedFor, rf.log, rf.offsetIndex, rf.offsetTerm} {
		if e.Encode(value) != nil {
			fmt.Printf("Encoder Failed\n")
			return
		}
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var offsetIndex int
	var offsetTerm int
	for _, value := range []interface{}{&currentTerm, &votedFor, &log, &offsetIndex, &offsetTerm} {
		if d.Decode(value) != nil {
			return
		}
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.offsetIndex = offsetIndex
	rf.offsetTerm = offsetTerm
}

func (rf *Raft) sendInstallSnapshot(servers int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	// Raft 之间通信
	ok := rf.peers[servers].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	// 拒绝过期的 Leader 请求
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionTimerLocked()

	// 比当前快照还旧，直接忽略
	if args.LastIncludedIndex <= rf.offsetIndex {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	// 只通知上层，不在 Raft 层修改状态
	// 状态变更由上层调用 CondInstallSnapshot 完成
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Raft 在发出 ApplyMsg 之后，可能又 apply 了更新的日志条目。
	// 如果上层已经 apply 到比这个 snapshot 更新的位置，拒绝安装。
	if lastIncludedIndex <= rf.lastApplied {
		return false
	}

	// snapshot 仍然是最新的，正式安装：裁剪日志
	if lastIncludedIndex < rf.offsetIndex+len(rf.log) {
		// log 中还有 lastIncludedIndex 之后的条目，保留
		rf.log = rf.log[rf.AbsoluteIndexToLogIndex(lastIncludedIndex):]
		rf.log[0].Term = lastIncludedTerm
	} else {
		// snapshot 覆盖了全部 log，重置为只含 dummy entry 的日志
		rf.log = make([]LogEntry, 1)
		rf.log[0].Term = lastIncludedTerm
	}

	rf.offsetIndex = lastIncludedIndex
	rf.offsetTerm = lastIncludedTerm
	rf.snapShot = snapshot

	// 更新 lastApplied / commitIndex，防止 applier 重复 apply 旧条目
	rf.lastApplied = lastIncludedIndex
	if rf.commitIndex < lastIncludedIndex {
		rf.commitIndex = lastIncludedIndex
	}

	rf.SaveStateAndSnapshot()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	// TODO:Client 对 raft 进行通知
	// 已经生成了 Snapshot，可以删除对应的旧日志，并持久化快照
	// Index 传入的为 Absolute Index, 即相对于整个日志的 Index
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.offsetIndex || index >= len(rf.log)+rf.offsetIndex {
		return
	}

	log := make([]LogEntry, 1, len(rf.log)-rf.AbsoluteIndexToLogIndex(index))
	log[0].Term = rf.log[rf.AbsoluteIndexToLogIndex(index)].Term
	rf.log = append(log, rf.log[rf.AbsoluteIndexToLogIndex(index)+1:]...)
	rf.offsetIndex = index
	rf.offsetTerm = log[0].Term
	rf.snapShot = snapshot
	rf.SaveStateAndSnapshot()
}

func (rf *Raft) SaveStateAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	for _, value := range []interface{}{rf.currentTerm, rf.votedFor, rf.log, rf.offsetIndex, rf.offsetTerm} {
		if e.Encode(value) != nil {
			fmt.Printf("Encoder Failed\n")
			return
		}
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapShot)
}

func (rf *Raft) readSnapshot(data []byte) ApplyMsg {
	if data == nil {
		return ApplyMsg{SnapshotValid: false}
	}
	// TODO: 读取 Snapshot 数据，并更新 Raft 的状态
	rf.snapShot = data
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  rf.offsetTerm,
		SnapshotIndex: rf.offsetIndex,
	}
	return msg
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	Leader            int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	lastIndex := len(rf.log) - 1 + rf.offsetIndex
	lastTerm := rf.log[rf.AbsoluteIndexToLogIndex(lastIndex)].Term

	logUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm &&
			args.LastLogIndex >= lastIndex)

	if logUpToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateID) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateID
		rf.resetElectionTimerLocked()
		rf.persist()
		return
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictTerm = 0
	reply.ConflictIndex = rf.offsetIndex
	if args.Term < rf.currentTerm {
		return
	}
	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex < rf.offsetIndex {
		reply.ConflictTerm = 0
		reply.ConflictIndex = rf.offsetIndex + 1
		return
	}
	if args.PrevLogIndex >= len(rf.log)+rf.offsetIndex {
		reply.ConflictTerm = 0 // 不存在
		reply.ConflictIndex = len(rf.log) + rf.offsetIndex
		return
	}
	if rf.log[rf.AbsoluteIndexToLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[rf.AbsoluteIndexToLogIndex(args.PrevLogIndex)].Term
		reply.ConflictIndex = args.PrevLogIndex 
		for reply.ConflictIndex > rf.offsetIndex && rf.log[rf.AbsoluteIndexToLogIndex(reply.ConflictIndex-1)].Term == reply.ConflictTerm {
			reply.ConflictIndex--
		}
		return
	}
	for i, log := range args.Entries {
		index := i + args.PrevLogIndex + 1
		if index < len(rf.log)+rf.offsetIndex {
			// Term 不同直接覆盖
			if rf.log[rf.AbsoluteIndexToLogIndex(index)].Term != log.Term {
				rf.log = rf.log[:rf.AbsoluteIndexToLogIndex(index)]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > len(rf.log)-1+rf.offsetIndex {
			rf.commitIndex = len(rf.log) - 1 + rf.offsetIndex
		}
	}
	reply.Success = true
	rf.persist()
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

func (rf *Raft) sendAppendEntries(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader = rf.currentTerm, (rf.state == Leader)
	if !isLeader {
		return -1, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	index = len(rf.log) - 1 + rf.offsetIndex
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.broadcastHeartbeatsLocked()
	rf.persist()
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

func (rf *Raft) resetElectionTimerLocked() {
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(400+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.state = Follower
	rf.persist()
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader
	for server := range rf.peers {
		rf.nextIndex[server] = len(rf.log) + rf.offsetIndex
		rf.matchIndex[server] = 0
	}
	rf.broadcastHeartbeatsLocked()
}

func (rf *Raft) startElectionLocked() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	rf.resetElectionTimerLocked()
	lastIndex := len(rf.log) - 1 + rf.offsetIndex
	args := RequestVoteArgs{
		Term: rf.currentTerm, CandidateID: rf.me,
		LastLogIndex: lastIndex, LastLogTerm: rf.log[rf.AbsoluteIndexToLogIndex(lastIndex)].Term,
	}
	votes := 1
	if votes > len(rf.peers)/2 {
		rf.becomeLeaderLocked()
		return
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.killed() {
				return
			}
			// 有更新的退回 Follower
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.resetElectionTimerLocked()
				return
			}
			// 成为 Leader or 退回 Follower || currentTerm 已经改变
			// || 回复的 Term 与本轮 Term 不同 选票无效
			if rf.state != Candidate || rf.currentTerm != args.Term || reply.Term != args.Term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeaderLocked()
				}
			}
		}(server)
	}
}

func (rf *Raft) broadcastHeartbeatsLocked() {
	// TODO: SendSnapshot
	rf.lastHeartbeatSent = time.Now()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		if rf.nextIndex[server] <= rf.offsetIndex {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				Leader:            rf.me,
				LastIncludedIndex: rf.offsetIndex,
				LastIncludedTerm:  rf.offsetTerm,
				Data:              rf.snapShot,
				Done:              true,
			}
			go rf.sendSnapshot(server, args)
			continue
		}

		prevLogIndex := rf.nextIndex[server] - 1
		log := rf.log[rf.AbsoluteIndexToLogIndex(rf.nextIndex[server]):]
		args := AppendEntriesArgs{
			Term: rf.currentTerm, LeaderID: rf.me,
			PrevLogIndex: prevLogIndex, PrevLogTerm: rf.log[rf.AbsoluteIndexToLogIndex(prevLogIndex)].Term, Entries: log,
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendHeartbeat(server, args)
	}
}

func (rf *Raft) sendSnapshot(server int, args InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	if !rf.sendInstallSnapshot(server, &args, &reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		rf.resetElectionTimerLocked()
		return
	}
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}
	// 更新该 Follower 的复制进度
	if rf.matchIndex[server] < args.LastIncludedIndex {
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	if rf.nextIndex[server] < args.LastIncludedIndex+1 {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
}

func (rf *Raft) sendHeartbeat(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if !rf.sendAppendEntries(server, &args, &reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果 reply 的 Term 高于自己的 currentTerm,自动退回 candidate 开启第二次选举
	if rf.killed() {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		rf.resetElectionTimerLocked()
		return
	}
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}
	if !reply.Success {
		if args.PrevLogIndex <= rf.matchIndex[server] || rf.nextIndex[server] != args.PrevLogIndex+1 {
			return
		}
		next := reply.ConflictIndex
		if reply.ConflictTerm != 0 {
			for index := len(rf.log) - 1 + rf.offsetIndex; index > rf.offsetIndex; index-- {
				if rf.log[rf.AbsoluteIndexToLogIndex(index)].Term == reply.ConflictTerm {
					next = index + 1
					break
				}
			}
		}
		if next < rf.matchIndex[server]+1 {
			next = rf.matchIndex[server] + 1
		}
		rf.nextIndex[server] = next
	} else {
		matched := args.PrevLogIndex + len(args.Entries)
		if matched > rf.matchIndex[server] {
			rf.matchIndex[server] = matched
		}
		if rf.nextIndex[server] < rf.matchIndex[server]+1 {
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	}
	for N := len(rf.log) - 1 + rf.offsetIndex; N > rf.commitIndex; N-- {
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= N && rf.log[rf.AbsoluteIndexToLogIndex(N)].Term == rf.currentTerm {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			break
		}
	}
}

// Check timers without waiting for any RPC to return.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			if time.Since(rf.lastHeartbeatSent) >= 100*time.Millisecond {
				rf.broadcastHeartbeatsLocked() // 广播 Heartbeat
			}
		} else if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
			rf.startElectionLocked() // 超时产生选举
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		var msgs []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.AbsoluteIndexToLogIndex(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			applyCh <- msg
		}
		time.Sleep(10 * time.Millisecond)
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
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.log = make([]LogEntry, 1)
	rf.offsetIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.resetElectionTimerLocked()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if msg := rf.readSnapshot(persister.ReadSnapshot()); msg.SnapshotValid {
		go func() { rf.applyCh <- msg }()
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	// Raft code apply msg to tester via channel
	go rf.applier(rf.applyCh)
	return rf
}
